"""
PostgreSQL Writer for PySpark CDC Consumer
Writes processed CDC data to PostgreSQL ODS with circuit breaker and retry
"""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from circuitbreaker import circuit, CircuitBreakerError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

from pyspark.sql import DataFrame

from ..config import PostgresConfig, ErrorHandlingConfig

logger = logging.getLogger(__name__)

# Table column mappings (PostgreSQL column names)
TABLE_COLUMNS = {
    "cdc_orders": [
        "order_id", "customer_id", "order_date", "total_amount",
        "status", "shipping_info", "source_ts", "operation"
    ],
    "cdc_order_items": [
        "item_id", "order_id", "product_id", "quantity",
        "unit_price", "subtotal", "source_ts", "operation"
    ],
    "cdc_order_items_extracted": [
        "order_id", "product_id", "quantity", "unit_price",
        "subtotal", "extracted_from", "source_ts"
    ],
    "cdc_customers": [
        "customer_id", "name", "email", "tier", "region",
        "created_at", "source_ts", "operation"
    ],
    "cdc_products": [
        "product_id", "name", "category", "price", "stock_qty",
        "is_active", "source_ts", "operation"
    ],
    "cdc_orders_enriched": [
        "order_id", "customer_id", "customer_name", "customer_tier",
        "customer_region", "total_amount", "item_count", "order_date",
        "status", "window_start", "window_end"
    ],
    "cdc_order_aggregations": [
        "window_start", "window_end", "customer_tier", "product_category",
        "order_count", "total_revenue", "avg_order_value"
    ],
    "cdc_audit_log": [
        "log_id", "table_name", "operation", "record_id",
        "changed_by", "changed_at", "event_type", "source_ts"
    ],
    "cdc_dlq": [
        "original_topic", "error_type", "error_message", "retry_count",
        "payload", "first_failure_at", "last_failure_at", "consumer_id", "status"
    ],
    "cdc_orders_dynamic": [
        "order_id", "customer_id", "data", "source_table",
        "operation", "event_timestamp"
    ],
}

# CDC source to PostgreSQL column name mapping
CDC_TO_PG_COLUMN_MAP = {
    "ORDERS": {
        "ORDER_ID": "order_id",
        "CUSTOMER_ID": "customer_id",
        "ORDER_DATE": "order_date",
        "TOTAL_AMOUNT": "total_amount",
        "STATUS": "status",
        "SHIPPING_INFO": "shipping_info",
    },
    "ORDER_ITEMS": {
        "ITEM_ID": "item_id",
        "ORDER_ID": "order_id",
        "PRODUCT_ID": "product_id",
        "QUANTITY": "quantity",
        "UNIT_PRICE": "unit_price",
        "SUBTOTAL": "subtotal",
    },
    "CUSTOMERS": {
        "CUSTOMER_ID": "customer_id",
        "FIRST_NAME": "name",  # Combined FIRST + LAST in processing
        "LAST_NAME": "_last_name",  # Will be combined
        "EMAIL": "email",
        "TIER": "tier",
        "CITY": "region",
    },
    "PRODUCTS": {
        "PRODUCT_ID": "product_id",
        "NAME": "name",
        "CATEGORY": "category",
        "PRICE": "price",
        "STOCK_QUANTITY": "stock_qty",
        "STATUS": "is_active",  # Will be mapped: ACTIVE->true
    },
    "AUDIT_LOG": {
        "LOG_ID": "log_id",
        "ACTION": "event_type",
        "ENTITY_TYPE": "table_name",
        "ENTITY_ID": "record_id",
        "USER_ID": "changed_by",
        "CREATED_AT": "changed_at",
    },
}


# Tables that use upsert (dimension/derived tables) with their conflict keys
UPSERT_TABLES = {
    "cdc_customers": ("customer_id",),
    "cdc_products": ("product_id",),
    "cdc_orders_enriched": ("order_id",),
}


class PostgresWriter:
    """Write CDC data to PostgreSQL ODS"""

    def __init__(self, config: PostgresConfig, error_config: ErrorHandlingConfig):
        self.config = config
        self.error_config = error_config
        self._conn = None
        self._failure_count = 0
        self._circuit_open = False
        self._circuit_open_time = 0

    def connect(self) -> bool:
        """Establish connection to PostgreSQL"""
        try:
            self._conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.database,
                user=self.config.username,
                password=self.config.password,
                connect_timeout=10
            )
            self._conn.autocommit = False
            logger.info(f"Connected to PostgreSQL at {self.config.host}:{self.config.port}")
            return True
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            return False

    def disconnect(self) -> None:
        """Close connection"""
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
            logger.info("Disconnected from PostgreSQL")

    def _ensure_connection(self) -> bool:
        """Ensure connection is alive, reconnect if needed"""
        if self._conn is None or self._conn.closed:
            return self.connect()
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception:
            self._conn = None
            return self.connect()

    def _check_circuit_breaker(self) -> bool:
        """Check if circuit breaker allows operations"""
        if not self._circuit_open:
            return True

        elapsed = time.time() - self._circuit_open_time
        if elapsed >= self.error_config.circuit_breaker.recovery_timeout:
            logger.info("Circuit breaker: attempting recovery")
            self._circuit_open = False
            self._failure_count = 0
            return True

        logger.warning(
            f"Circuit breaker OPEN, "
            f"{self.error_config.circuit_breaker.recovery_timeout - elapsed:.0f}s until retry"
        )
        return False

    def _record_failure(self) -> None:
        """Record a failure for circuit breaker"""
        self._failure_count += 1
        if self._failure_count >= self.error_config.circuit_breaker.failure_threshold:
            self._circuit_open = True
            self._circuit_open_time = time.time()
            logger.error(
                f"Circuit breaker OPENED after {self._failure_count} failures"
            )

    def _record_success(self) -> None:
        """Record success, reset failure count"""
        self._failure_count = 0

    def write_batch(
        self,
        table: str,
        records: List[Dict[str, Any]]
    ) -> int:
        """
        Write batch of records to PostgreSQL table.
        Returns number of records written.
        """
        if not records:
            return 0

        if not self._check_circuit_breaker():
            raise CircuitBreakerError("Circuit breaker is open")

        if not self._ensure_connection():
            self._record_failure()
            raise ConnectionError("Cannot connect to PostgreSQL")

        columns = TABLE_COLUMNS.get(table)
        if not columns:
            logger.error(f"Unknown table: {table}")
            return 0

        try:
            if table in UPSERT_TABLES:
                return self._execute_batch_upsert(table, columns, records)
            return self._execute_batch_insert(table, columns, records)
        except psycopg2.errors.UniqueViolation:
            # Duplicate - not a system failure
            self._conn.rollback()
            logger.warning(f"Duplicate records in batch for {table}, inserting one by one")
            return self._execute_single_inserts(table, columns, records)
        except psycopg2.OperationalError as e:
            self._conn.rollback()
            self._record_failure()
            raise
        except Exception as e:
            if self._conn and not self._conn.closed:
                self._conn.rollback()
            self._record_failure()
            raise

    def _execute_batch_insert(
        self, table: str, columns: List[str], records: List[Dict[str, Any]]
    ) -> int:
        """Execute batch insert using execute_values"""
        placeholders = ", ".join(["%s"] * len(columns))
        col_str = ", ".join(columns)
        sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"

        rows = []
        for record in records:
            row = tuple(record.get(col) for col in columns)
            rows.append(row)

        with self._conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=self.config.batch_size)

        self._conn.commit()
        self._record_success()
        logger.debug(f"Inserted {len(rows)} records into {table}")
        return len(rows)

    def _execute_batch_upsert(
        self, table: str, columns: List[str], records: List[Dict[str, Any]]
    ) -> int:
        """Execute batch upsert (INSERT ... ON CONFLICT DO UPDATE) for dimension tables"""
        conflict_keys = UPSERT_TABLES[table]
        update_cols = [c for c in columns if c not in conflict_keys]

        col_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        conflict_str = ", ".join(conflict_keys)
        update_str = ", ".join(
            f"{col} = EXCLUDED.{col}" for col in update_cols
        )

        sql = (
            f"INSERT INTO {table} ({col_str}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"
        )

        rows = []
        for record in records:
            row = tuple(record.get(col) for col in columns)
            rows.append(row)

        with self._conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=self.config.batch_size)

        self._conn.commit()
        self._record_success()
        logger.debug(f"Upserted {len(rows)} records into {table}")
        return len(rows)

    def _execute_single_inserts(
        self, table: str, columns: List[str], records: List[Dict[str, Any]]
    ) -> int:
        """Insert records one by one, skipping duplicates"""
        placeholders = ", ".join(["%s"] * len(columns))
        col_str = ", ".join(columns)
        sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"

        inserted = 0
        for record in records:
            try:
                row = tuple(record.get(col) for col in columns)
                with self._conn.cursor() as cur:
                    cur.execute(sql, row)
                self._conn.commit()
                inserted += 1
            except psycopg2.errors.UniqueViolation:
                self._conn.rollback()
            except Exception as e:
                self._conn.rollback()
                logger.error(f"Failed to insert record: {e}")

        self._record_success()
        return inserted

    def write_dlq(
        self,
        original_topic: str,
        error_type: str,
        error_message: str,
        payload: Dict[str, Any],
        consumer_id: str = "spark-consumer-1"
    ) -> bool:
        """Write failed record to DLQ table"""
        if not self._ensure_connection():
            logger.error("Cannot write to DLQ - no connection")
            return False

        try:
            now = datetime.now()
            record = {
                "original_topic": original_topic,
                "error_type": error_type,
                "error_message": error_message[:1000],  # Truncate
                "retry_count": 0,
                "payload": json.dumps(payload),
                "first_failure_at": now,
                "last_failure_at": now,
                "consumer_id": consumer_id,
                "status": "pending"
            }

            columns = TABLE_COLUMNS["cdc_dlq"]
            placeholders = ", ".join(["%s"] * len(columns))
            col_str = ", ".join(columns)
            sql = f"INSERT INTO cdc_dlq ({col_str}) VALUES ({placeholders})"

            row = tuple(record.get(col) for col in columns)
            with self._conn.cursor() as cur:
                cur.execute(sql, row)
            self._conn.commit()

            logger.info(f"DLQ record written: {error_type} for {original_topic}")
            return True

        except Exception as e:
            if self._conn and not self._conn.closed:
                self._conn.rollback()
            logger.error(f"Failed to write DLQ: {e}")
            return False

    def write_checkpoint(
        self,
        consumer_id: str,
        topic: str,
        offset: int,
        timestamp: Optional[datetime] = None
    ) -> bool:
        """Update consumer checkpoint"""
        if not self._ensure_connection():
            return False

        try:
            sql = """
                INSERT INTO cdc_checkpoints (consumer_id, topic, partition_id, last_offset, last_timestamp, updated_at)
                VALUES (%s, %s, 0, %s, %s, %s)
                ON CONFLICT (consumer_id, topic, partition_id)
                DO UPDATE SET last_offset = EXCLUDED.last_offset,
                             last_timestamp = EXCLUDED.last_timestamp,
                             updated_at = EXCLUDED.updated_at
            """
            now = datetime.now()
            with self._conn.cursor() as cur:
                cur.execute(sql, (consumer_id, topic, offset, timestamp or now, now))
            self._conn.commit()
            return True
        except Exception as e:
            if self._conn and not self._conn.closed:
                self._conn.rollback()
            logger.error(f"Failed to write checkpoint: {e}")
            return False

    def write_dynamic(
        self,
        order_id: int,
        customer_id: int,
        data: Dict[str, Any],
        source_table: str,
        operation: str,
        event_timestamp: Optional[datetime] = None
    ) -> bool:
        """Write to dynamic schema table (JSONB)"""
        if not self._ensure_connection():
            return False

        try:
            record = {
                "order_id": order_id,
                "customer_id": customer_id,
                "data": json.dumps(data),
                "source_table": source_table,
                "operation": operation,
                "event_timestamp": event_timestamp or datetime.now()
            }
            self.write_batch("cdc_orders_dynamic", [record])
            return True
        except Exception as e:
            logger.error(f"Failed to write dynamic record: {e}")
            return False

    def get_stats(self) -> Dict[str, Any]:
        """Get writer statistics"""
        return {
            "connected": self._conn is not None and not self._conn.closed,
            "failure_count": self._failure_count,
            "circuit_open": self._circuit_open,
        }

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
