"""
PySpark CDC Consumer Main Entry Point
Orchestrates the full CDC processing pipeline:
  Solace -> Parse -> Deduplicate -> Join -> XML Extract -> PostgreSQL + REST API
"""

import json
import logging
import signal
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession

from .config import get_config, SparkConsumerConfig
from .solace_source import SolaceSource, CDCMessage
from .dlq_handler import DLQHandler
from .state_store import (
    DimensionStateStore, OrderItemsStateStore, bootstrap_dimension_state
)
from .transformations.schema_handler import SchemaHandler
from .transformations.cdc_processor import CDCProcessor, TABLE_KEY_MAP
from .transformations.xml_extractor import XMLExtractor
from .transformations.stream_joiner import StreamJoiner
from .writers.postgres_writer import PostgresWriter, CDC_TO_PG_COLUMN_MAP
from .writers.rest_sink import RestApiSink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# CDC table -> PostgreSQL table mapping
TABLE_PG_MAP = {
    "ORDERS": "cdc_orders",
    "ORDER_ITEMS": "cdc_order_items",
    "CUSTOMERS": "cdc_customers",
    "PRODUCTS": "cdc_products",
    "AUDIT_LOG": "cdc_audit_log",
}


class SparkCDCConsumer:
    """Main PySpark CDC Consumer orchestrator"""

    def __init__(self, config: SparkConsumerConfig):
        self.config = config
        self.running = False

        # Initialize Spark
        self.spark = self._create_spark_session()

        # Initialize components
        self.source = SolaceSource(config.solace)
        self.writer = PostgresWriter(config.postgres, config.error_handling)
        self.dlq_handler = DLQHandler(config.error_handling.dlq)
        self.schema_handler = SchemaHandler(writer=self.writer)
        self.cdc_processor = CDCProcessor(self.spark)
        self.xml_extractor = XMLExtractor(self.spark, config.xml.column)
        self.joiner = StreamJoiner(self.spark, config.join)

        # Initialize REST API sink (second sink)
        self.rest_sink: Optional[RestApiSink] = None
        if config.rest_api.enabled:
            self.rest_sink = RestApiSink(
                base_url=config.rest_api.base_url,
                error_config=config.error_handling,
                consumer_id=config.rest_api.consumer_id,
                timeout=config.rest_api.timeout,
            )

        # Wire DLQ to writer
        self.dlq_handler.set_writer(self.writer)

        # Keyed state stores for dimension tables (replace list buffers)
        state_ttl = self._parse_ttl(config.join.state_ttl)
        self._customer_state = DimensionStateStore("customers", ttl_seconds=state_ttl)
        self._product_state = DimensionStateStore("products", ttl_seconds=state_ttl)
        self._order_items_state = OrderItemsStateStore(ttl_seconds=state_ttl)

        # Broadcast variables (refreshed periodically)
        self._customer_broadcast = None
        self._product_broadcast = None
        self._broadcast_refresh_interval = 60  # seconds
        self._last_broadcast_refresh = 0.0

        # Stats
        self._batches_processed = 0
        self._total_events = 0

        # Signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        builder = SparkSession.builder \
            .appName(self.config.spark.app_name) \
            .master(self.config.spark.master) \
            .config("spark.driver.memory", self.config.spark.driver_memory) \
            .config("spark.executor.memory", self.config.spark.executor_memory) \
            .config("spark.executor.cores", self.config.spark.executor_cores) \
            .config("spark.driver.maxResultSize", self.config.spark.max_result_size) \
            .config("spark.sql.shuffle.partitions", self.config.spark.shuffle_partitions) \
            .config("spark.sql.streaming.checkpointLocation", self.config.streaming.checkpoint_location) \
            .config("spark.ui.port", self.config.spark.ui_port) \
            .config("spark.sql.adaptive.enabled", "true")

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Spark session created: {self.config.spark.app_name}")
        return spark

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False

    def run(self) -> None:
        """Main consumer loop"""
        logger.info("=" * 60)
        logger.info("PySpark CDC Consumer Starting")
        logger.info("=" * 60)

        # Connect to Solace
        if not self.source.connect():
            logger.error("Failed to connect to Solace, exiting")
            sys.exit(1)

        # Connect to PostgreSQL
        if not self.writer.connect():
            logger.error("Failed to connect to PostgreSQL, exiting")
            self.source.disconnect()
            sys.exit(1)

        # Load column mappings from cdc_schema_registry (falls back to hardcoded)
        self.writer.load_mappings_from_db()

        # Bootstrap dimension state from PostgreSQL (crash recovery)
        self._bootstrap_state()

        self.running = True
        trigger_seconds = self._parse_interval(self.config.streaming.trigger_interval)
        last_stats_time = time.time()

        logger.info(f"Trigger interval: {self.config.streaming.trigger_interval}")
        logger.info(f"Checkpoint: {self.config.streaming.checkpoint_location}")
        logger.info("-" * 60)

        try:
            while self.running:
                batch_start = time.time()

                # Poll messages from Solace
                messages_by_table = self.source.get_messages_by_table(
                    max_batch=self.config.postgres.batch_size
                )

                if messages_by_table:
                    self._process_batch(messages_by_table)
                    self._batches_processed += 1

                # Periodic stats
                if time.time() - last_stats_time >= 30:
                    self._log_stats()
                    last_stats_time = time.time()

                # Rate control
                elapsed = time.time() - batch_start
                sleep_time = max(0, trigger_seconds - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        finally:
            self._shutdown()

    def _process_batch(self, messages_by_table: Dict[str, List[CDCMessage]]) -> None:
        """Process a batch of messages grouped by table"""
        for table, messages in messages_by_table.items():
            try:
                self._process_table_batch(table, messages)
                self._total_events += len(messages)
            except Exception as e:
                logger.error(f"Error processing batch for {table}: {e}")
                # Route all messages to DLQ
                for msg in messages:
                    self.dlq_handler.route_to_dlq(
                        original_topic=msg.topic,
                        error_type=DLQHandler.PROCESSING_ERROR,
                        error_message=str(e),
                        payload=msg.payload
                    )

        # Attempt enrichment join if we have orders in this batch
        if "ORDERS" in messages_by_table:
            self._attempt_enrichment_join(messages_by_table)

        # Flush DLQ
        self.dlq_handler.flush()

    def _process_table_batch(self, table: str, messages: List[CDCMessage]) -> None:
        """Process messages for a single table"""
        pg_table = TABLE_PG_MAP.get(table)
        if not pg_table:
            logger.warning(f"No PostgreSQL mapping for table: {table}")
            return

        # Extract payloads
        payloads = [msg.payload for msg in messages]

        # Process through CDC processor
        key_col = TABLE_KEY_MAP.get(table, f"{table}_ID")
        df = self.cdc_processor.process_batch(table, payloads, key_col)

        if df is None:
            return

        # Schema evolution: register new columns
        for msg in messages:
            data = msg.after or msg.before or {}
            self.schema_handler.register_columns(table, data)

        # Convert to records for PostgreSQL
        records = self._df_to_pg_records(df, table)

        # Handle XML extraction for ORDERS
        xml_items = []
        if table == "ORDERS" and self.config.xml.enabled:
            records, xml_items = self._handle_xml_extraction(records, messages)
            if xml_items:
                try:
                    self.writer.write_batch("cdc_order_items_extracted", xml_items)
                    logger.info(f"Wrote {len(xml_items)} XML-extracted items to PG")
                except Exception as e:
                    logger.error(f"Failed to write XML items to PG: {e}")

        # Handle dynamic schema (extra columns -> JSONB table)
        dynamic_records = self._extract_dynamic_records(table, messages)
        if dynamic_records:
            try:
                self.writer.write_batch("cdc_orders_dynamic", dynamic_records)
            except Exception as e:
                logger.warning(f"Failed to write dynamic records: {e}")

        # Sink 1: Write to PostgreSQL (ODS)
        if records:
            try:
                written = self.writer.write_batch(pg_table, records)
                logger.debug(f"[PG] Wrote {written} records to {pg_table}")
            except Exception as e:
                logger.error(f"[PG] Failed to write to {pg_table}: {e}")
                for msg in messages:
                    self.dlq_handler.route_to_dlq(
                        original_topic=msg.topic,
                        error_type=DLQHandler.WRITE_ERROR,
                        error_message=str(e),
                        payload=msg.payload
                    )

        # Sink 2: Push to REST API
        if self.rest_sink and records:
            try:
                self.rest_sink.send_events_batch(table, records)
                logger.debug(f"[REST] Pushed {len(records)} records for {table}")
            except Exception as e:
                logger.error(f"[REST] Failed to push {table}: {e}")

            # Also push extracted XML items to REST API
            if xml_items:
                try:
                    self.rest_sink.send_extracted_items(xml_items)
                    logger.debug(f"[REST] Pushed {len(xml_items)} XML items")
                except Exception as e:
                    logger.error(f"[REST] Failed to push XML items: {e}")

        # Update dimension state for joins
        self._update_dimension_state(table, messages)

    def _df_to_pg_records(
        self, df, table: str
    ) -> List[Dict[str, Any]]:
        """Convert Spark DataFrame to PostgreSQL-compatible records"""
        column_map = CDC_TO_PG_COLUMN_MAP.get(table, {})
        rows = df.collect()
        records = []

        for row in rows:
            record = {}
            row_dict = row.asDict()

            for cdc_col, pg_col in column_map.items():
                if cdc_col in row_dict:
                    record[pg_col] = row_dict[cdc_col]

            # Add CDC metadata
            record["source_ts"] = row_dict.get("source_ts")
            record["operation"] = row_dict.get("operation")

            # Special handling for CUSTOMERS (combine first/last name)
            if table == "CUSTOMERS":
                first = row_dict.get("FIRST_NAME", "")
                last = row_dict.get("LAST_NAME", "")
                record["name"] = f"{first} {last}".strip()
                record.pop("_last_name", None)

            # Special handling for PRODUCTS (status -> is_active)
            if table == "PRODUCTS":
                status = row_dict.get("STATUS", "ACTIVE")
                record["is_active"] = status == "ACTIVE"
                record["stock_qty"] = row_dict.get("STOCK_QUANTITY")

            records.append(record)

        return records

    def _handle_xml_extraction(
        self, records: List[Dict[str, Any]], messages: List[CDCMessage]
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Extract XML items from order records"""
        cleaned = []
        extracted_items = []

        for record in records:
            xml_data = record.get("shipping_info")
            if xml_data:
                try:
                    from .transformations.xml_extractor import parse_shipping_xml
                    items = parse_shipping_xml(xml_data)
                    if items:
                        order_id = record.get("order_id")
                        for item in items:
                            item["order_id"] = order_id
                            item["extracted_from"] = "SHIPPING_INFO_XML"
                            item["source_ts"] = record.get("source_ts")
                            extracted_items.append(item)
                except Exception as e:
                    # XML parse failure -> DLQ
                    self.dlq_handler.route_to_dlq(
                        original_topic=f"cdc/oracle/SALES/ORDERS",
                        error_type=DLQHandler.MALFORMED_XML,
                        error_message=str(e),
                        payload=record
                    )
            cleaned.append(record)

        return cleaned, extracted_items

    def _extract_dynamic_records(
        self, table: str, messages: List[CDCMessage]
    ) -> List[Dict[str, Any]]:
        """Extract records with extra columns for dynamic schema table"""
        dynamic_records = []
        for msg in messages:
            data = msg.after or msg.before or {}
            prepared = self.schema_handler.prepare_dynamic_record(table, data)
            if prepared["has_extra"]:
                dynamic_records.append({
                    "order_id": data.get("ORDER_ID", 0),
                    "customer_id": data.get("CUSTOMER_ID", 0),
                    "data": json.dumps(prepared["extra"]),
                    "source_table": table,
                    "operation": msg.operation,
                    "event_timestamp": msg.source_datetime
                })
        return dynamic_records

    def _update_dimension_state(self, table: str, messages: List[CDCMessage]) -> None:
        """Update keyed dimension state stores from incoming CDC messages"""
        for msg in messages:
            data = msg.after or msg.before or {}
            data["_cdc_ts_ms"] = msg.source_ts

            if table == "CUSTOMERS":
                key = data.get("CUSTOMER_ID")
                if key is not None:
                    self._customer_state.put(key, data)
            elif table == "PRODUCTS":
                key = data.get("PRODUCT_ID")
                if key is not None:
                    self._product_state.put(key, data)
            elif table == "ORDER_ITEMS":
                order_id = data.get("ORDER_ID")
                if order_id is not None:
                    self._order_items_state.add_item(order_id, data)

    def _attempt_enrichment_join(
        self, messages_by_table: Dict[str, List[CDCMessage]]
    ) -> None:
        """Attempt 3-way join for order enrichment using broadcast state"""
        try:
            orders_msgs = messages_by_table.get("ORDERS", [])
            if not orders_msgs:
                return

            # Build orders DataFrame from current batch
            orders_data = [m.after or m.before or {} for m in orders_msgs]
            for d in orders_data:
                d["_cdc_ts_ms"] = int(time.time() * 1000)

            orders_df = self.cdc_processor.parse_events("ORDERS", [
                {"payload": {"after": d, "op": "c", "ts_ms": d.get("_cdc_ts_ms", 0), "source": {"table": "ORDERS"}}}
                for d in orders_data
            ])
            orders_df = self.cdc_processor.add_processing_metadata(orders_df)

            # Refresh broadcast variables if needed
            self._refresh_broadcasts()

            # Customers from keyed state (already deduped, latest per key)
            customers_data = self._customer_state.get_all_values()
            customers_df = self.spark.createDataFrame(
                customers_data
            ) if customers_data else self.spark.createDataFrame([], "string")

            # Order items from keyed state (grouped by order_id)
            items_data = self._order_items_state.get_all_items_flat()
            order_items_df = self.spark.createDataFrame(
                items_data
            ) if items_data else self.spark.createDataFrame([], "string")

            # Perform enrichment join
            enriched = self.joiner.enrich_orders(
                orders_df, customers_df, order_items_df
            )

            if enriched and not enriched.rdd.isEmpty():
                enriched_records = [row.asDict() for row in enriched.collect()]

                # Sink 1: PostgreSQL
                self.writer.write_batch("cdc_orders_enriched", enriched_records)
                logger.info(f"[PG] Wrote {len(enriched_records)} enriched orders")

                # Sink 2: REST API
                if self.rest_sink:
                    try:
                        self.rest_sink.send_enriched_orders(enriched_records)
                        logger.info(f"[REST] Pushed {len(enriched_records)} enriched orders")
                    except Exception as e:
                        logger.error(f"[REST] Failed to push enriched orders: {e}")

                # Aggregations
                aggregated = self.joiner.aggregate_orders(enriched)
                if aggregated and not aggregated.rdd.isEmpty():
                    agg_records = [row.asDict() for row in aggregated.collect()]
                    self.writer.write_batch("cdc_order_aggregations", agg_records)

        except Exception as e:
            logger.error(f"Enrichment join failed: {e}")

    def _bootstrap_state(self) -> None:
        """Bootstrap dimension state from PostgreSQL (crash recovery)"""
        logger.info("Bootstrapping dimension state from PostgreSQL...")
        if self.writer._conn is None or self.writer._conn.closed:
            logger.warning("No PG connection for bootstrap, starting with empty state")
            return

        stats = bootstrap_dimension_state(
            conn=self.writer._conn,
            customer_store=self._customer_state,
            product_store=self._product_state,
            order_items_store=self._order_items_state,
        )
        logger.info(
            f"Bootstrap loaded: {stats['customers']} customers, "
            f"{stats['products']} products, {stats['order_items']} order items"
        )

    def _refresh_broadcasts(self) -> None:
        """Refresh Spark broadcast variables for dimension data"""
        now = time.time()
        if now - self._last_broadcast_refresh < self._broadcast_refresh_interval:
            return

        # Destroy old broadcasts
        if self._customer_broadcast is not None:
            self._customer_broadcast.destroy()
        if self._product_broadcast is not None:
            self._product_broadcast.destroy()

        # Create new broadcasts from current state
        self._customer_broadcast = self.spark.sparkContext.broadcast(
            self._customer_state.get_all()
        )
        self._product_broadcast = self.spark.sparkContext.broadcast(
            self._product_state.get_all()
        )
        self._last_broadcast_refresh = now
        logger.debug(
            f"Broadcast refreshed: {self._customer_state.size()} customers, "
            f"{self._product_state.size()} products"
        )

    @staticmethod
    def _parse_ttl(ttl_str: str) -> int:
        """Parse TTL string like '1 hour' to seconds"""
        parts = ttl_str.lower().split()
        if len(parts) == 2:
            value, unit = float(parts[0]), parts[1]
            if unit.startswith("second"):
                return int(value)
            elif unit.startswith("minute"):
                return int(value * 60)
            elif unit.startswith("hour"):
                return int(value * 3600)
        return 3600  # default 1 hour

    def _parse_interval(self, interval: str) -> float:
        """Parse interval string to seconds"""
        parts = interval.lower().split()
        if len(parts) == 2:
            value, unit = float(parts[0]), parts[1]
            if unit.startswith("second"):
                return value
            elif unit.startswith("minute"):
                return value * 60
            elif unit.startswith("hour"):
                return value * 3600
        return 10.0  # Default 10 seconds

    def _log_stats(self) -> None:
        """Log processing statistics"""
        source_stats = self.source.get_stats()
        writer_stats = self.writer.get_stats()
        dlq_stats = self.dlq_handler.get_stats()

        rest_info = ""
        if self.rest_sink:
            rest_stats = self.rest_sink.get_stats()
            rest_info = (
                f", rest_sent={rest_stats.get('events_sent', 0)}, "
                f"rest_failed={rest_stats.get('events_failed', 0)}, "
                f"rest_circuit={'OPEN' if rest_stats.get('circuit_open') else 'CLOSED'}"
            )

        logger.info(
            f"Stats: batches={self._batches_processed}, "
            f"events={self._total_events}, "
            f"queue_depth={source_stats.get('queue_depth', 0)}, "
            f"dlq_errors={dlq_stats.get('total_errors', 0)}, "
            f"pg_circuit={'OPEN' if writer_stats.get('circuit_open') else 'CLOSED'}"
            f"{rest_info}, "
            f"state=[customers={self._customer_state.size()}, "
            f"products={self._product_state.size()}, "
            f"order_items={self._order_items_state.total_items()}]"
        )

    def _shutdown(self) -> None:
        """Graceful shutdown"""
        logger.info("-" * 60)
        logger.info("Shutting down PySpark CDC Consumer...")

        # Flush remaining DLQ records
        self.dlq_handler.flush()

        # Final stats
        self._log_stats()

        # Disconnect
        self.source.disconnect()
        self.writer.disconnect()

        # Close REST API sink
        if self.rest_sink:
            self.rest_sink.close()

        # Stop Spark
        if self.spark:
            self.spark.stop()

        logger.info("PySpark CDC Consumer stopped")


def main():
    """Entry point"""
    config = get_config()
    logging.getLogger().setLevel(config.log_level)

    consumer = SparkCDCConsumer(config)
    consumer.run()


if __name__ == "__main__":
    main()
