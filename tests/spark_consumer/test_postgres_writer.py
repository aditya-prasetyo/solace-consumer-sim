"""
Tests for PySpark PostgreSQL Writer
Covers: circuit breaker, batch insert, single insert fallback, DLQ write, column mappings
"""

import time
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from src.spark_consumer.config import PostgresConfig, ErrorHandlingConfig, CircuitBreakerConfig
from src.spark_consumer.writers.postgres_writer import (
    PostgresWriter,
    TABLE_COLUMNS,
    CDC_TO_PG_COLUMN_MAP,
)


@pytest.fixture
def pg_config():
    return PostgresConfig(
        host="localhost", port=5432, database="test_ods",
        username="test", password="test", batch_size=100,
    )


@pytest.fixture
def error_config():
    return ErrorHandlingConfig(
        circuit_breaker=CircuitBreakerConfig(failure_threshold=3, recovery_timeout=2),
    )


@pytest.fixture
def writer(pg_config, error_config):
    return PostgresWriter(pg_config, error_config)


class TestTableMappings:
    """Verify table column definitions are correct"""

    def test_orders_columns(self):
        cols = TABLE_COLUMNS["cdc_orders"]
        assert "order_id" in cols
        assert "customer_id" in cols
        assert "total_amount" in cols
        assert "source_ts" in cols
        assert "operation" in cols

    def test_customers_columns(self):
        cols = TABLE_COLUMNS["cdc_customers"]
        assert "customer_id" in cols
        assert "name" in cols
        assert "tier" in cols

    def test_enriched_columns(self):
        cols = TABLE_COLUMNS["cdc_orders_enriched"]
        assert "order_id" in cols
        assert "customer_name" in cols
        assert "customer_tier" in cols
        assert "item_count" in cols

    def test_dlq_columns(self):
        cols = TABLE_COLUMNS["cdc_dlq"]
        assert "original_topic" in cols
        assert "error_type" in cols
        assert "payload" in cols
        assert "status" in cols

    def test_cdc_to_pg_map_orders(self):
        mapping = CDC_TO_PG_COLUMN_MAP["ORDERS"]
        assert mapping["ORDER_ID"] == "order_id"
        assert mapping["CUSTOMER_ID"] == "customer_id"
        assert mapping["TOTAL_AMOUNT"] == "total_amount"

    def test_cdc_to_pg_map_customers(self):
        mapping = CDC_TO_PG_COLUMN_MAP["CUSTOMERS"]
        assert mapping["CUSTOMER_ID"] == "customer_id"
        assert mapping["TIER"] == "tier"

    def test_all_tables_have_mappings(self):
        expected_tables = {"ORDERS", "ORDER_ITEMS", "CUSTOMERS", "PRODUCTS", "AUDIT_LOG"}
        assert set(CDC_TO_PG_COLUMN_MAP.keys()) == expected_tables


class TestPostgresWriterCircuitBreaker:

    def test_circuit_initially_closed(self, writer):
        assert writer._circuit_open is False
        assert writer._check_circuit_breaker() is True

    def test_circuit_opens_after_threshold(self, writer):
        for _ in range(3):
            writer._record_failure()

        assert writer._circuit_open is True
        assert writer._check_circuit_breaker() is False

    def test_circuit_recovers_after_timeout(self, writer):
        for _ in range(3):
            writer._record_failure()

        time.sleep(2.1)
        assert writer._check_circuit_breaker() is True
        assert writer._failure_count == 0

    def test_success_resets_failures(self, writer):
        writer._record_failure()
        writer._record_failure()
        writer._record_success()
        assert writer._failure_count == 0


class TestPostgresWriterBatch:

    def test_write_batch_empty(self, writer):
        assert writer.write_batch("cdc_orders", []) == 0

    def test_write_batch_unknown_table(self, writer):
        writer._conn = MagicMock()
        writer._conn.closed = False
        assert writer.write_batch("nonexistent_table", [{"a": 1}]) == 0

    @patch.object(PostgresWriter, '_ensure_connection', return_value=True)
    @patch.object(PostgresWriter, '_execute_batch_insert', return_value=5)
    def test_write_batch_success(self, mock_insert, mock_conn, writer):
        records = [{"order_id": i} for i in range(5)]
        result = writer.write_batch("cdc_orders", records)
        assert result == 5

    def test_write_batch_circuit_open(self, writer):
        for _ in range(3):
            writer._record_failure()

        from circuitbreaker import CircuitBreakerError
        with pytest.raises(CircuitBreakerError):
            writer.write_batch("cdc_orders", [{"order_id": 1}])


class TestPostgresWriterDLQ:

    @patch.object(PostgresWriter, '_ensure_connection', return_value=True)
    def test_write_dlq(self, mock_conn, writer):
        mock_cursor = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_cursor)
        mock_context.__exit__ = MagicMock(return_value=False)

        writer._conn = MagicMock()
        writer._conn.closed = False
        writer._conn.cursor.return_value = mock_context

        result = writer.write_dlq(
            original_topic="cdc/oracle/SALES/ORDERS",
            error_type="MALFORMED_JSON",
            error_message="Parse error",
            payload={"raw": "data"},
        )
        assert result is True


class TestPostgresWriterStats:

    def test_initial_stats(self, writer):
        stats = writer.get_stats()
        assert stats["connected"] is False
        assert stats["failure_count"] == 0
        assert stats["circuit_open"] is False
