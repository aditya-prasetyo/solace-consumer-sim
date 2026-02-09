"""
Tests for PyFlink REST API Sink
Covers: circuit breaker, send methods, health check, stats
"""

import time
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.flink_consumer.config import (
    RestApiSinkConfig, ErrorHandlingConfig, CircuitBreakerConfig,
)
from src.flink_consumer.sinks.rest_sink import FlinkRestApiSink, _dt_str


@pytest.fixture
def rest_config():
    return RestApiSinkConfig(
        url="http://localhost:8000",
        consumer_id="flink-test-1",
        timeout=5,
        retry_attempts=1,
    )


@pytest.fixture
def error_config():
    return ErrorHandlingConfig(
        circuit_breaker=CircuitBreakerConfig(failure_threshold=3, recovery_timeout=2),
    )


@pytest.fixture
def sink(rest_config, error_config):
    return FlinkRestApiSink(rest_config, error_config)


class TestFlinkRestSinkCircuitBreaker:

    def test_circuit_initially_closed(self, sink):
        assert sink._circuit_open is False

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


class TestFlinkRestSinkSend:

    @patch.object(FlinkRestApiSink, '_post', return_value=True)
    def test_send_events_batch(self, mock_post, sink):
        events = [{"ORDER_ID": 1001, "_cdc_op": "c", "_source_ts": "2024-01-15T00:00:00"}]
        result = sink.send_events_batch("ORDERS", events)

        assert result is True
        url = mock_post.call_args[0][0]
        assert "/api/v1/events/batch" in url

    @patch.object(FlinkRestApiSink, '_post', return_value=True)
    def test_send_enriched_orders(self, mock_post, sink):
        orders = [{"order_id": 1001, "customer_name": "John", "total_amount": 299.99}]
        result = sink.send_enriched_orders(orders)

        assert result is True
        url = mock_post.call_args[0][0]
        assert "/api/v1/enriched-orders/batch" in url

    @patch.object(FlinkRestApiSink, '_post', return_value=True)
    def test_send_extracted_items(self, mock_post, sink):
        items = [{"order_id": 1001, "product_id": 10, "quantity": 2}]
        result = sink.send_extracted_items(items)

        assert result is True
        url = mock_post.call_args[0][0]
        assert "/api/v1/order-items/batch" in url

    @patch.object(FlinkRestApiSink, '_post', return_value=True)
    def test_send_notification(self, mock_post, sink):
        result = sink.send_notification(
            event_type="HIGH_VALUE_ORDER",
            message="Order 1001 is high value",
            severity="warning",
        )
        assert result is True
        url = mock_post.call_args[0][0]
        assert "/api/v1/notifications" in url

    def test_send_empty_events(self, sink):
        assert sink.send_events_batch("ORDERS", []) is False
        assert sink.send_enriched_orders([]) is False
        assert sink.send_extracted_items([]) is False

    def test_send_blocked_by_circuit(self, sink):
        for _ in range(3):
            sink._record_failure()
        assert sink.send_events_batch("ORDERS", [{"data": 1}]) is False


class TestFlinkRestSinkHealth:

    @patch('requests.Session.get')
    def test_health_check_ok(self, mock_get, sink):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_get.return_value = mock_resp
        assert sink.health_check() is True

    @patch('requests.Session.get', side_effect=Exception("Connection refused"))
    def test_health_check_fail(self, mock_get, sink):
        assert sink.health_check() is False


class TestFlinkRestSinkStats:

    def test_initial_stats(self, sink):
        stats = sink.get_stats()
        assert stats["sent"] == 0
        assert stats["failed"] == 0
        assert stats["circuit_open"] is False


class TestDtStr:

    def test_datetime(self):
        dt = datetime(2024, 1, 15, 10, 30, 0)
        assert _dt_str(dt) == "2024-01-15T10:30:00"

    def test_none(self):
        assert _dt_str(None) is None

    def test_string(self):
        assert _dt_str("2024-01-15") == "2024-01-15"
