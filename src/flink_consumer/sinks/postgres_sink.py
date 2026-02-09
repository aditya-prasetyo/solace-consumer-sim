"""
PostgreSQL Sink for PyFlink CDC Consumer
Writes processed CDC data to PostgreSQL ODS with circuit breaker
"""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

from ..config import PostgresSinkConfig, ErrorHandlingConfig

logger = logging.getLogger(__name__)

# CDC source to PostgreSQL column mapping
CDC_TO_PG_MAP = {
    "ORDERS": {
        "table": "cdc_orders",
        "columns": ["order_id", "customer_id", "order_date", "total_amount",
                     "status", "shipping_info", "source_ts", "operation"],
        "field_map": {
            "ORDER_ID": "order_id", "CUSTOMER_ID": "customer_id",
            "ORDER_DATE": "order_date", "TOTAL_AMOUNT": "total_amount",
            "STATUS": "status", "SHIPPING_INFO": "shipping_info",
        },
    },
    "ORDER_ITEMS": {
        "table": "cdc_order_items",
        "columns": ["item_id", "order_id", "product_id", "quantity",
                     "unit_price", "subtotal", "source_ts", "operation"],
        "field_map": {
            "ITEM_ID": "item_id", "ORDER_ID": "order_id",
            "PRODUCT_ID": "product_id", "QUANTITY": "quantity",
            "UNIT_PRICE": "unit_price", "SUBTOTAL": "subtotal",
        },
    },
    "CUSTOMERS": {
        "table": "cdc_customers",
        "columns": ["customer_id", "name", "email", "tier", "region",
                     "created_at", "source_ts", "operation"],
        "field_map": {
            "CUSTOMER_ID": "customer_id", "EMAIL": "email",
            "TIER": "tier", "CITY": "region", "CREATED_AT": "created_at",
        },
        "special": "customer_name",
    },
    "PRODUCTS": {
        "table": "cdc_products",
        "columns": ["product_id", "name", "category", "price", "stock_qty",
                     "is_active", "source_ts", "operation"],
        "field_map": {
            "PRODUCT_ID": "product_id", "NAME": "name",
            "CATEGORY": "category", "PRICE": "price",
            "STOCK_QUANTITY": "stock_qty",
        },
        "special": "product_status",
    },
    "AUDIT_LOG": {
        "table": "cdc_audit_log",
        "columns": ["log_id", "table_name", "operation", "record_id",
                     "changed_by", "changed_at", "event_type", "source_ts"],
        "field_map": {
            "LOG_ID": "log_id", "ENTITY_TYPE": "table_name",
            "ENTITY_ID": "record_id", "USER_ID": "changed_by",
            "CREATED_AT": "changed_at", "ACTION": "event_type",
        },
    },
}

ENRICHED_COLUMNS = [
    "order_id", "customer_id", "customer_name", "customer_tier",
    "customer_region", "total_amount", "item_count", "order_date",
    "status", "window_start", "window_end",
]

AGGREGATION_COLUMNS = [
    "window_start", "window_end", "customer_tier", "product_category",
    "order_count", "total_revenue", "avg_order_value",
]

EXTRACTED_ITEM_COLUMNS = [
    "order_id", "product_id", "quantity", "unit_price",
    "subtotal", "extracted_from", "source_ts",
]

DLQ_COLUMNS = [
    "original_topic", "error_type", "error_message", "retry_count",
    "payload", "first_failure_at", "last_failure_at", "consumer_id", "status",
]


class FlinkPostgresSink:
    """PostgreSQL ODS sink for PyFlink consumer"""

    def __init__(self, config: PostgresSinkConfig, error_config: ErrorHandlingConfig):
        self.config = config
        self.error_config = error_config
        self._conn = None
        self._failure_count = 0
        self._circuit_open = False
        self._circuit_open_time = 0.0
        self._total_written = 0

    def connect(self) -> bool:
        try:
            self._conn = psycopg2.connect(
                host=self.config.host, port=self.config.port,
                dbname=self.config.database,
                user=self.config.username, password=self.config.password,
                connect_timeout=10,
            )
            self._conn.autocommit = False
            logger.info(f"Connected to PostgreSQL at {self.config.host}:{self.config.port}")
            return True
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            return False

    def disconnect(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _ensure_connection(self) -> bool:
        if self._conn is None or self._conn.closed:
            return self.connect()
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception:
            self._conn = None
            return self.connect()

    def _check_circuit(self) -> bool:
        if not self._circuit_open:
            return True
        if time.time() - self._circuit_open_time >= self.error_config.circuit_breaker.recovery_timeout:
            self._circuit_open = False
            self._failure_count = 0
            return True
        return False

    def _record_failure(self) -> None:
        self._failure_count += 1
        if self._failure_count >= self.error_config.circuit_breaker.failure_threshold:
            self._circuit_open = True
            self._circuit_open_time = time.time()
            logger.error(f"PG circuit breaker OPENED after {self._failure_count} failures")

    def _record_success(self) -> None:
        self._failure_count = 0

    def write_cdc_records(
        self, table: str, records: List[Dict[str, Any]], operation: str = "c"
    ) -> int:
        """Write CDC records for a specific table"""
        mapping = CDC_TO_PG_MAP.get(table)
        if not mapping:
            logger.warning(f"No PG mapping for table: {table}")
            return 0

        pg_table = mapping["table"]
        columns = mapping["columns"]
        field_map = mapping["field_map"]

        pg_records = []
        for r in records:
            pg_row = {}
            for cdc_col, pg_col in field_map.items():
                pg_row[pg_col] = r.get(cdc_col)

            # Add metadata
            pg_row["source_ts"] = r.get("_source_ts")
            pg_row["operation"] = r.get("_cdc_op", operation)

            # Special handling
            special = mapping.get("special")
            if special == "customer_name":
                first = r.get("FIRST_NAME", "")
                last = r.get("LAST_NAME", "")
                pg_row["name"] = f"{first} {last}".strip()
            elif special == "product_status":
                pg_row["is_active"] = r.get("STATUS", "ACTIVE") == "ACTIVE"

            pg_records.append(pg_row)

        return self._batch_insert(pg_table, columns, pg_records)

    def write_enriched(self, records: List[Dict[str, Any]]) -> int:
        """Write enriched order records"""
        return self._batch_insert("cdc_orders_enriched", ENRICHED_COLUMNS, records)

    def write_aggregations(self, records: List[Dict[str, Any]]) -> int:
        return self._batch_insert("cdc_order_aggregations", AGGREGATION_COLUMNS, records)

    def write_extracted_items(self, items: List[Dict[str, Any]]) -> int:
        return self._batch_insert("cdc_order_items_extracted", EXTRACTED_ITEM_COLUMNS, items)

    def write_dlq(
        self, topic: str, error_type: str, error_msg: str,
        payload: Dict[str, Any], consumer_id: str = "flink-consumer-1"
    ) -> bool:
        now = datetime.now()
        record = {
            "original_topic": topic,
            "error_type": error_type,
            "error_message": error_msg[:1000],
            "retry_count": 0,
            "payload": json.dumps(payload),
            "first_failure_at": now,
            "last_failure_at": now,
            "consumer_id": consumer_id,
            "status": "pending",
        }
        return self._batch_insert("cdc_dlq", DLQ_COLUMNS, [record]) > 0

    def _batch_insert(
        self, table: str, columns: List[str], records: List[Dict[str, Any]]
    ) -> int:
        if not records or not self._check_circuit():
            return 0
        if not self._ensure_connection():
            self._record_failure()
            return 0

        placeholders = ", ".join(["%s"] * len(columns))
        col_str = ", ".join(columns)
        sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"

        try:
            rows = [tuple(r.get(c) for c in columns) for r in records]
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql, rows, page_size=self.config.batch_size)
            self._conn.commit()
            self._record_success()
            self._total_written += len(rows)
            return len(rows)
        except psycopg2.errors.UniqueViolation:
            self._conn.rollback()
            return self._insert_one_by_one(sql, columns, records)
        except Exception as e:
            if self._conn and not self._conn.closed:
                self._conn.rollback()
            self._record_failure()
            logger.error(f"PG batch insert failed for {table}: {e}")
            return 0

    def _insert_one_by_one(
        self, sql: str, columns: List[str], records: List[Dict[str, Any]]
    ) -> int:
        inserted = 0
        for r in records:
            try:
                row = tuple(r.get(c) for c in columns)
                with self._conn.cursor() as cur:
                    cur.execute(sql, row)
                self._conn.commit()
                inserted += 1
            except psycopg2.errors.UniqueViolation:
                self._conn.rollback()
            except Exception:
                self._conn.rollback()
        self._record_success()
        self._total_written += inserted
        return inserted

    def get_stats(self) -> Dict[str, Any]:
        return {
            "connected": self._conn is not None and not self._conn.closed,
            "total_written": self._total_written,
            "failure_count": self._failure_count,
            "circuit_open": self._circuit_open,
        }

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()
