"""
Tests for PyFlink DLQ Handler
Covers: routing, flush to PG + REST, buffer, stats, disabled DLQ
"""

import pytest
from unittest.mock import MagicMock

from src.flink_consumer.config import DLQConfig
from src.flink_consumer.dlq_handler import FlinkDLQHandler


class TestFlinkDLQHandler:

    def test_route_buffers_record(self):
        handler = FlinkDLQHandler(DLQConfig(enabled=True))
        handler.route("cdc/oracle/SALES/ORDERS", "MALFORMED_JSON", "Bad JSON", {"raw": "data"})

        stats = handler.get_stats()
        assert stats["total_errors"] == 1
        assert stats["buffer_size"] == 1

    def test_route_disabled(self):
        handler = FlinkDLQHandler(DLQConfig(enabled=False))
        handler.route("topic", "ERROR", "msg", {})

        assert handler.get_stats()["total_errors"] == 0
        assert handler.get_stats()["buffer_size"] == 0

    def test_flush_to_pg_sink(self):
        handler = FlinkDLQHandler(DLQConfig(enabled=True))
        mock_pg = MagicMock()
        handler.set_pg_sink(mock_pg)

        handler.route("topic1", "ERROR1", "msg1", {"k": "v"})
        handler.route("topic2", "ERROR2", "msg2", {"k": "v"})

        flushed = handler.flush()
        assert flushed == 2
        assert mock_pg.write_dlq.call_count == 2

    def test_flush_also_notifies_rest(self):
        handler = FlinkDLQHandler(DLQConfig(enabled=True))
        mock_pg = MagicMock()
        mock_rest = MagicMock()
        handler.set_pg_sink(mock_pg)
        handler.set_rest_sink(mock_rest)

        handler.route("topic", "ERROR", "msg", {})
        handler.flush()

        mock_rest.send_notification.assert_called_once()
        call_kwargs = mock_rest.send_notification.call_args
        assert call_kwargs[1]["event_type"] == "DLQ_ENTRY" or call_kwargs[0][0] == "DLQ_ENTRY"

    def test_flush_empty(self):
        handler = FlinkDLQHandler(DLQConfig(enabled=True))
        assert handler.flush() == 0

    def test_flush_pg_error_doesnt_crash(self):
        handler = FlinkDLQHandler(DLQConfig(enabled=True))
        mock_pg = MagicMock()
        mock_pg.write_dlq.side_effect = Exception("DB error")
        handler.set_pg_sink(mock_pg)

        handler.route("topic", "ERROR", "msg", {})
        flushed = handler.flush()

        assert flushed == 0
        assert handler.get_stats()["buffer_size"] == 0

    def test_auto_flush_at_50(self):
        handler = FlinkDLQHandler(DLQConfig(enabled=True))
        mock_pg = MagicMock()
        handler.set_pg_sink(mock_pg)

        for i in range(50):
            handler.route(f"topic_{i}", "ERROR", "msg", {})

        # Auto-flush triggered
        assert handler.get_stats()["buffer_size"] == 0
        assert mock_pg.write_dlq.call_count == 50

    def test_error_type_constants(self):
        assert FlinkDLQHandler.MALFORMED_JSON == "MALFORMED_JSON"
        assert FlinkDLQHandler.MALFORMED_XML == "MALFORMED_XML"
        assert FlinkDLQHandler.PROCESSING_ERROR == "PROCESSING_ERROR"
        assert FlinkDLQHandler.WRITE_ERROR == "WRITE_ERROR"
        assert FlinkDLQHandler.SCHEMA_MISMATCH == "SCHEMA_MISMATCH"

    def test_consumer_id(self):
        handler = FlinkDLQHandler(DLQConfig(enabled=True), consumer_id="flink-test-1")
        assert handler.consumer_id == "flink-test-1"
