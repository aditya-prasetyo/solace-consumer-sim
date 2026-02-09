"""
Tests for PyFlink PostgreSQL Sink
Covers: circuit breaker, CDC record mapping, enriched/aggregation/extracted writes, DLQ write
"""

import time
import pytest
from unittest.mock import MagicMock, patch

from src.flink_consumer.config import (
    PostgresSinkConfig, ErrorHandlingConfig, CircuitBreakerConfig,
)
from src.flink_consumer.sinks.postgres_sink import (
    FlinkPostgresSink,
    CDC_TO_PG_MAP,
    ENRICHED_COLUMNS,
    AGGREGATION_COLUMNS,
    EXTRACTED_ITEM_COLUMNS,
    DLQ_COLUMNS,
)


@pytest.fixture
def sink_config():
    return PostgresSinkConfig(
        host="localhost", port=5432, database="test_ods",
        username="test", password="test", batch_size=100,
    )


@pytest.fixture
def error_config():
    return ErrorHandlingConfig(
        circuit_breaker=CircuitBreakerConfig(failure_threshold=3, recovery_timeout=2),
    )


@pytest.fixture
def sink(sink_config, error_config):
    return FlinkPostgresSink(sink_config, error_config)


class TestCDCColumnMappings:

    def test_orders_mapping(self):
        mapping = CDC_TO_PG_MAP["ORDERS"]
        assert mapping["table"] == "cdc_orders"
        assert "order_id" in mapping["columns"]
        assert mapping["field_map"]["ORDER_ID"] == "order_id"

    def test_customers_mapping(self):
        mapping = CDC_TO_PG_MAP["CUSTOMERS"]
        assert mapping["table"] == "cdc_customers"
        assert mapping.get("special") == "customer_name"

    def test_products_mapping(self):
        mapping = CDC_TO_PG_MAP["PRODUCTS"]
        assert mapping["table"] == "cdc_products"
        assert mapping.get("special") == "product_status"

    def test_order_items_mapping(self):
        mapping = CDC_TO_PG_MAP["ORDER_ITEMS"]
        assert mapping["table"] == "cdc_order_items"

    def test_enriched_columns(self):
        assert "order_id" in ENRICHED_COLUMNS
        assert "customer_name" in ENRICHED_COLUMNS
        assert "customer_tier" in ENRICHED_COLUMNS
        assert "item_count" in ENRICHED_COLUMNS

    def test_aggregation_columns(self):
        assert "customer_tier" in AGGREGATION_COLUMNS
        assert "order_count" in AGGREGATION_COLUMNS
        assert "total_revenue" in AGGREGATION_COLUMNS

    def test_dlq_columns(self):
        assert "original_topic" in DLQ_COLUMNS
        assert "error_type" in DLQ_COLUMNS
        assert "status" in DLQ_COLUMNS


class TestFlinkPGSinkCircuitBreaker:

    def test_circuit_initially_closed(self, sink):
        assert sink._circuit_open is False
        assert sink._check_circuit() is True

    def test_circuit_opens_after_threshold(self, sink):
        for _ in range(3):
            sink._record_failure()

        assert sink._circuit_open is True
        assert sink._check_circuit() is False

    def test_circuit_recovers(self, sink):
        for _ in range(3):
            sink._record_failure()

        time.sleep(2.1)
        assert sink._check_circuit() is True

    def test_success_resets_failures(self, sink):
        sink._record_failure()
        sink._record_failure()
        sink._record_success()
        assert sink._failure_count == 0


class TestFlinkPGSinkWrite:

    def test_write_cdc_records_unknown_table(self, sink):
        assert sink.write_cdc_records("UNKNOWN_TABLE", [{"data": 1}]) == 0

    def test_write_cdc_records_empty(self, sink):
        assert sink.write_cdc_records("ORDERS", []) == 0

    @patch.object(FlinkPostgresSink, '_batch_insert', return_value=3)
    def test_write_cdc_records_orders(self, mock_insert, sink):
        records = [
            {
                "ORDER_ID": 1001, "CUSTOMER_ID": 501,
                "TOTAL_AMOUNT": 299.99, "STATUS": "PENDING",
                "_cdc_op": "c", "_source_ts": "2024-01-15T00:00:00",
            }
        ]
        result = sink.write_cdc_records("ORDERS", records)

        mock_insert.assert_called_once()
        call_args = mock_insert.call_args
        assert call_args[0][0] == "cdc_orders"

    @patch.object(FlinkPostgresSink, '_batch_insert', return_value=1)
    def test_write_cdc_records_customers_name_combine(self, mock_insert, sink):
        """Customer FIRST_NAME + LAST_NAME combined to name"""
        records = [
            {
                "CUSTOMER_ID": 501, "FIRST_NAME": "John", "LAST_NAME": "Doe",
                "EMAIL": "john@test.com", "TIER": "GOLD", "CITY": "Jakarta",
                "_cdc_op": "c", "_source_ts": "2024-01-15T00:00:00",
            }
        ]
        sink.write_cdc_records("CUSTOMERS", records)

        pg_records = mock_insert.call_args[0][2]
        assert pg_records[0]["name"] == "John Doe"

    @patch.object(FlinkPostgresSink, '_batch_insert', return_value=1)
    def test_write_cdc_records_products_status_to_bool(self, mock_insert, sink):
        """Product STATUS mapped to is_active boolean"""
        records = [
            {
                "PRODUCT_ID": 10, "NAME": "Widget", "CATEGORY": "Electronics",
                "PRICE": 49.99, "STOCK_QUANTITY": 100, "STATUS": "ACTIVE",
                "_cdc_op": "c", "_source_ts": "2024-01-15T00:00:00",
            }
        ]
        sink.write_cdc_records("PRODUCTS", records)

        pg_records = mock_insert.call_args[0][2]
        assert pg_records[0]["is_active"] is True

    @patch.object(FlinkPostgresSink, '_batch_insert', return_value=5)
    def test_write_enriched(self, mock_insert, sink):
        records = [{"order_id": 1001, "customer_name": "John"}]
        result = sink.write_enriched(records)
        assert result == 5
        assert mock_insert.call_args[0][0] == "cdc_orders_enriched"

    @patch.object(FlinkPostgresSink, '_batch_insert', return_value=3)
    def test_write_aggregations(self, mock_insert, sink):
        records = [{"customer_tier": "GOLD", "order_count": 5}]
        result = sink.write_aggregations(records)
        assert result == 3

    @patch.object(FlinkPostgresSink, '_batch_insert', return_value=2)
    def test_write_extracted_items(self, mock_insert, sink):
        items = [{"order_id": 1001, "product_id": 10}]
        result = sink.write_extracted_items(items)
        assert result == 2


class TestFlinkPGSinkStats:

    def test_initial_stats(self, sink):
        stats = sink.get_stats()
        assert stats["connected"] is False
        assert stats["total_written"] == 0
        assert stats["circuit_open"] is False
