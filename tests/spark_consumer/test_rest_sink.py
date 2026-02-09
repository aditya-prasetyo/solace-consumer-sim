"""
Tests for PySpark REST API Sink
Covers: circuit breaker, send_events_batch, send_enriched_orders, send_notification, health_check
"""

import time
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.spark_consumer.config import ErrorHandlingConfig, CircuitBreakerConfig, RetryConfig
from src.spark_consumer.writers.rest_sink import RestApiSink, _serialize_dt


@pytest.fixture
def error_config():
    return ErrorHandlingConfig(
        retry=RetryConfig(max_attempts=1, initial_delay_ms=100, max_delay_ms=1000, multiplier=1.0),
        circuit_breaker=CircuitBreakerConfig(failure_threshold=3, recovery_timeout=2),
    )


@pytest.fixture
def sink(error_config):
    return RestApiSink(
        base_url="http://localhost:8000",
        error_config=error_config,
        consumer_id="test-consumer",
        timeout=5,
    )


class TestRestApiSinkCircuitBreaker:

    def test_circuit_initially_closed(self, sink):
        assert sink._circuit_open is False
        assert sink._check_circuit() is True

    def test_circuit_opens_after_threshold(self, sink):
        for _ in range(3):
            sink._record_failure()

        assert sink._circuit_open is True
        assert sink._check_circuit() is False

    def test_circuit_recovers_after_timeout(self, sink):
        for _ in range(3):
            sink._record_failure()

        assert sink._circuit_open is True

        time.sleep(2.1)
        assert sink._check_circuit() is True
        assert sink._circuit_open is False

    def test_success_resets_failure_count(self, sink):
        sink._record_failure()
        sink._record_failure()
        sink._record_success()

        assert sink._failure_count == 0


class TestRestApiSinkSend:

    @patch.object(RestApiSink, '_post', return_value=True)
    def test_send_events_batch(self, mock_post, sink):
        events = [
            {"ORDER_ID": 1, "operation": "c", "source_ts": "2024-01-01"},
        ]
        result = sink.send_events_batch("ORDERS", events)

        assert result is True
        mock_post.assert_called_once()
        call_url = mock_post.call_args[0][0]
        assert "/api/v1/events/batch" in call_url

    @patch.object(RestApiSink, '_post', return_value=True)
    def test_send_enriched_orders(self, mock_post, sink):
        orders = [
            {"order_id": 1001, "customer_id": 501, "total_amount": 299.99},
        ]
        result = sink.send_enriched_orders(orders)

        assert result is True
        mock_post.assert_called_once()
        call_url = mock_post.call_args[0][0]
        assert "/api/v1/enriched-orders/batch" in call_url

    @patch.object(RestApiSink, '_post', return_value=True)
    def test_send_extracted_items(self, mock_post, sink):
        items = [
            {"order_id": 1001, "product_id": 10, "quantity": 2},
        ]
        result = sink.send_extracted_items(items)

        assert result is True
        call_url = mock_post.call_args[0][0]
        assert "/api/v1/order-items/batch" in call_url

    @patch.object(RestApiSink, '_post', return_value=True)
    def test_send_notification(self, mock_post, sink):
        result = sink.send_notification(
            event_type="HIGH_VALUE_ORDER",
            message="Order 1001 amount $10,000",
            severity="warning",
            payload={"order_id": 1001},
        )
        assert result is True
        call_url = mock_post.call_args[0][0]
        assert "/api/v1/notifications" in call_url

    def test_send_empty_events(self, sink):
        assert sink.send_events_batch("ORDERS", []) is False
        assert sink.send_enriched_orders([]) is False
        assert sink.send_extracted_items([]) is False

    def test_send_blocked_by_circuit(self, sink):
        for _ in range(3):
            sink._record_failure()

        assert sink.send_events_batch("ORDERS", [{"data": 1}]) is False


class TestRestApiSinkHealthCheck:

    @patch('requests.Session.get')
    def test_health_check_success(self, mock_get, sink):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        assert sink.health_check() is True

    @patch('requests.Session.get', side_effect=Exception("Connection refused"))
    def test_health_check_failure(self, mock_get, sink):
        assert sink.health_check() is False


class TestRestApiSinkStats:

    def test_initial_stats(self, sink):
        stats = sink.get_stats()
        assert stats["events_sent"] == 0
        assert stats["events_failed"] == 0
        assert stats["circuit_open"] is False
        assert stats["base_url"] == "http://localhost:8000"


class TestSerializeDt:

    def test_serialize_datetime(self):
        dt = datetime(2024, 1, 15, 12, 30, 0)
        assert _serialize_dt(dt) == "2024-01-15T12:30:00"

    def test_serialize_none(self):
        assert _serialize_dt(None) is None

    def test_serialize_string(self):
        assert _serialize_dt("2024-01-15") == "2024-01-15"
