"""
PyFlink CDC Consumer Main Entry Point
Orchestrates the real-time CDC processing pipeline:
  Solace -> Parse -> Deduplicate -> Join -> XML Extract -> PostgreSQL + REST API
"""

import logging
import signal
import sys
import time
from typing import Any, Dict, List, Optional

from .config import get_config, FlinkConsumerConfig
from .solace_source import FlinkSolaceSource, CDCRecord
from .processors.cdc_processor import CDCEventParser, Deduplicator, TABLE_KEY_MAP
from .processors.order_enrichment import OrderEnrichmentProcessor
from .processors.xml_extractor import extract_batch
from .sinks.postgres_sink import FlinkPostgresSink
from .sinks.rest_sink import FlinkRestApiSink
from .dlq_handler import FlinkDLQHandler
from ..spark_consumer.state_store import bootstrap_dimension_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class FlinkCDCConsumer:
    """Main PyFlink CDC Consumer orchestrator with dual sinks"""

    def __init__(self, config: FlinkConsumerConfig):
        self.config = config
        self.running = False

        # Core components
        self.source = FlinkSolaceSource(config.solace)
        self.parser = CDCEventParser()
        self.dedup = Deduplicator()
        self.enrichment = OrderEnrichmentProcessor(state_ttl_ms=config.state.ttl_ms)

        # Sink 1: PostgreSQL ODS
        self.pg_sink: Optional[FlinkPostgresSink] = None
        if config.postgres.enabled:
            self.pg_sink = FlinkPostgresSink(config.postgres, config.error_handling)

        # Sink 2: REST API
        self.rest_sink: Optional[FlinkRestApiSink] = None
        if config.rest_api.enabled:
            self.rest_sink = FlinkRestApiSink(config.rest_api, config.error_handling)

        # DLQ
        self.dlq = FlinkDLQHandler(config.error_handling.dlq, "flink-consumer-1")
        if self.pg_sink:
            self.dlq.set_pg_sink(self.pg_sink)
        if self.rest_sink:
            self.dlq.set_rest_sink(self.rest_sink)

        # Stats
        self._batches = 0
        self._total_events = 0

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info(f"Signal {signum} received, shutting down...")
        self.running = False

    def run(self) -> None:
        """Main consumer loop"""
        logger.info("=" * 60)
        logger.info("PyFlink CDC Consumer Starting (Dual Sink)")
        logger.info("=" * 60)

        # Connect to Solace
        if not self.source.connect():
            logger.error("Failed to connect to Solace")
            sys.exit(1)

        # Connect to PostgreSQL (Sink 1)
        if self.pg_sink:
            if not self.pg_sink.connect():
                logger.error("Failed to connect to PostgreSQL")
                self.source.disconnect()
                sys.exit(1)
            logger.info(f"[Sink 1] PostgreSQL: {self.config.postgres.host}:{self.config.postgres.port}")

        # Bootstrap dimension state from PostgreSQL (crash recovery)
        if self.pg_sink:
            self._bootstrap_state()

        # Check REST API (Sink 2)
        if self.rest_sink:
            if self.rest_sink.health_check():
                logger.info(f"[Sink 2] REST API: {self.config.rest_api.url} (healthy)")
            else:
                logger.warning(f"[Sink 2] REST API: {self.config.rest_api.url} (not reachable, will retry)")

        self.running = True
        poll_interval = self.config.postgres.batch_interval
        last_stats = time.time()

        logger.info(f"Poll interval: {poll_interval}s")
        logger.info("-" * 60)

        try:
            while self.running:
                batch_start = time.time()

                # Poll messages from Solace
                records_by_table = self.source.poll_by_table(
                    max_batch=self.config.postgres.batch_size
                )

                if records_by_table:
                    self._process_batch(records_by_table)
                    self._batches += 1

                # Periodic stats
                if time.time() - last_stats >= 30:
                    self._log_stats()
                    last_stats = time.time()

                # Rate control
                elapsed = time.time() - batch_start
                sleep = max(0, poll_interval - elapsed)
                if sleep > 0:
                    time.sleep(sleep)

        except KeyboardInterrupt:
            pass
        finally:
            self._shutdown()

    def _process_batch(self, records_by_table: Dict[str, List[CDCRecord]]) -> None:
        """Process a batch grouped by table"""
        for table, records in records_by_table.items():
            try:
                self._process_table(table, records)
                self._total_events += len(records)
            except Exception as e:
                logger.error(f"Batch error for {table}: {e}")
                for r in records:
                    self.dlq.route(r.topic, FlinkDLQHandler.PROCESSING_ERROR, str(e), r.payload)

        # Enrichment join when ORDERS present
        if "ORDERS" in records_by_table:
            self._do_enrichment(records_by_table)

        self.dlq.flush()

    def _process_table(self, table: str, records: List[CDCRecord]) -> None:
        """Process records for a single table"""
        # Parse CDC events
        parsed = []
        for r in records:
            result = self.parser.parse(r.payload, table)
            if result:
                parsed.append(result)
            else:
                self.dlq.route(r.topic, FlinkDLQHandler.MALFORMED_JSON, "Parse failed", r.payload)

        if not parsed:
            return

        # Deduplicate
        parsed = self.dedup.deduplicate_batch(table, parsed)

        # Update enrichment state
        if table == "CUSTOMERS":
            for r in parsed:
                self.enrichment.update_customer(r)
        elif table == "PRODUCTS":
            for r in parsed:
                self.enrichment.update_product(r)
        elif table == "ORDER_ITEMS":
            for r in parsed:
                self.enrichment.add_order_item(r)

        # XML extraction for ORDERS
        xml_items = []
        if table == "ORDERS" and self.config.xml.enabled:
            parsed, xml_items, xml_errors = extract_batch(parsed, self.config.xml.column)

            # Route XML errors to DLQ
            for err in xml_errors:
                self.dlq.route(
                    f"cdc/oracle/SALES/ORDERS",
                    FlinkDLQHandler.MALFORMED_XML,
                    err["error"],
                    err["record"],
                )

        # Sink 1: PostgreSQL
        if self.pg_sink:
            try:
                written = self.pg_sink.write_cdc_records(table, parsed)
                logger.debug(f"[PG] {table}: {written} records")
            except Exception as e:
                logger.error(f"[PG] Write failed for {table}: {e}")

            if xml_items:
                try:
                    self.pg_sink.write_extracted_items(xml_items)
                    logger.debug(f"[PG] XML items: {len(xml_items)}")
                except Exception as e:
                    logger.error(f"[PG] XML items write failed: {e}")

        # Sink 2: REST API
        if self.rest_sink:
            try:
                self.rest_sink.send_events_batch(table, parsed)
                logger.debug(f"[REST] {table}: {len(parsed)} events")
            except Exception as e:
                logger.error(f"[REST] Push failed for {table}: {e}")

            if xml_items:
                try:
                    self.rest_sink.send_extracted_items(xml_items)
                except Exception as e:
                    logger.error(f"[REST] XML items push failed: {e}")

    def _do_enrichment(self, records_by_table: Dict[str, List[CDCRecord]]) -> None:
        """Perform 3-way join enrichment for orders"""
        try:
            order_records = records_by_table.get("ORDERS", [])
            if not order_records:
                return

            # Parse orders
            orders = []
            for r in order_records:
                parsed = self.parser.parse(r.payload, "ORDERS")
                if parsed:
                    orders.append(parsed)

            if not orders:
                return

            # Enrich with customer + item data
            enriched = self.enrichment.enrich_batch(orders)
            if not enriched:
                return

            # Aggregations
            aggregations = self.enrichment.aggregate_by_tier(enriched)

            # Sink 1: PostgreSQL
            if self.pg_sink:
                try:
                    self.pg_sink.write_enriched(enriched)
                    logger.info(f"[PG] Enriched: {len(enriched)} orders")
                except Exception as e:
                    logger.error(f"[PG] Enriched write failed: {e}")

                if aggregations:
                    try:
                        self.pg_sink.write_aggregations(aggregations)
                    except Exception as e:
                        logger.error(f"[PG] Aggregation write failed: {e}")

            # Sink 2: REST API
            if self.rest_sink:
                try:
                    self.rest_sink.send_enriched_orders(enriched)
                    logger.info(f"[REST] Enriched: {len(enriched)} orders")
                except Exception as e:
                    logger.error(f"[REST] Enriched push failed: {e}")

            # Alert: high-value orders
            if self.config.alerts.high_value_enabled and self.rest_sink:
                for o in enriched:
                    amount = o.get("total_amount") or 0
                    if amount >= self.config.alerts.high_value_threshold:
                        self.rest_sink.send_notification(
                            event_type="HIGH_VALUE_ORDER",
                            message=f"Order {o.get('order_id')} amount ${amount:,.2f}",
                            severity="warning",
                            payload={"order_id": o.get("order_id"), "amount": amount},
                        )

        except Exception as e:
            logger.error(f"Enrichment failed: {e}")

    def _bootstrap_state(self) -> None:
        """Bootstrap dimension state from PostgreSQL (crash recovery)"""
        logger.info("Bootstrapping dimension state from PostgreSQL...")
        conn = self.pg_sink._conn
        if conn is None or conn.closed:
            logger.warning("No PG connection for bootstrap, starting with empty state")
            return

        stats = bootstrap_dimension_state(
            conn=conn,
            customer_store=self.enrichment.get_customer_store(),
            product_store=self.enrichment.get_product_store(),
            order_items_store=self.enrichment.get_order_items_store(),
        )
        logger.info(
            f"Bootstrap loaded: {stats['customers']} customers, "
            f"{stats['products']} products, {stats['order_items']} order items"
        )

    def _log_stats(self) -> None:
        src = self.source.get_stats()
        dlq = self.dlq.get_stats()

        pg_info = ""
        if self.pg_sink:
            pg = self.pg_sink.get_stats()
            pg_info = f", pg_written={pg['total_written']}, pg_circuit={'OPEN' if pg['circuit_open'] else 'CLOSED'}"

        rest_info = ""
        if self.rest_sink:
            rest = self.rest_sink.get_stats()
            rest_info = f", rest_sent={rest['sent']}, rest_circuit={'OPEN' if rest['circuit_open'] else 'CLOSED'}"

        state = self.enrichment.get_state_stats()

        logger.info(
            f"Stats: batches={self._batches}, events={self._total_events}, "
            f"queue={src.get('queue_depth', 0)}, dlq={dlq['total_errors']}, "
            f"state=[cust={state['customers']},prod={state['products']},"
            f"items={state.get('order_items_total', 0)}]"
            f"{pg_info}{rest_info}"
        )

    def _shutdown(self) -> None:
        logger.info("-" * 60)
        logger.info("Shutting down PyFlink CDC Consumer...")

        self.dlq.flush()
        self._log_stats()

        self.source.disconnect()
        if self.pg_sink:
            self.pg_sink.disconnect()
        if self.rest_sink:
            self.rest_sink.close()

        logger.info("PyFlink CDC Consumer stopped")


def main():
    config = get_config()
    logging.getLogger().setLevel(config.log_level)
    consumer = FlinkCDCConsumer(config)
    consumer.run()


if __name__ == "__main__":
    main()
