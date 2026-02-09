"""
Tests for PySpark DLQ Handler
Covers: routing, buffering, auto-flush, flush to writer, stats, disabled DLQ
"""

import pytest
from unittest.mock import MagicMock

from src.spark_consumer.config import DLQConfig
from src.spark_consumer.dlq_handler import DLQHandler, DLQRecord


class TestDLQRecord:
    """Tests for DLQRecord data class"""

    def test_to_dict(self):
        record = DLQRecord(
            original_topic="cdc/oracle/SALES/ORDERS/INSERT",
            error_type="MALFORMED_JSON",
            error_message="Parse error at line 1",
            payload={"raw": "bad json"},
        )

        d = record.to_dict()
        assert d["original_topic"] == "cdc/oracle/SALES/ORDERS/INSERT"
        assert d["error_type"] == "MALFORMED_JSON"
        assert d["status"] == "pending"
        assert d["retry_count"] == 0
        assert '"raw"' in d["payload"]  # JSON serialized

    def test_error_message_truncated(self):
        record = DLQRecord(
            original_topic="test",
            error_type="ERROR",
            error_message="x" * 2000,
            payload={},
        )
        d = record.to_dict()
        assert len(d["error_message"]) == 1000


class TestDLQHandler:
    """Tests for DLQ Handler"""

    def test_route_to_dlq_buffers(self):
        handler = DLQHandler(DLQConfig(enabled=True))
        handler.route_to_dlq(
            "cdc/oracle/SALES/ORDERS/INSERT",
            DLQHandler.MALFORMED_JSON,
            "Bad JSON",
            {"raw": "data"},
        )

        stats = handler.get_stats()
        assert stats["total_errors"] == 1
        assert stats["buffer_size"] == 1

    def test_route_disabled_dlq(self):
        handler = DLQHandler(DLQConfig(enabled=False))
        handler.route_to_dlq("topic", "ERROR", "msg", {})

        stats = handler.get_stats()
        assert stats["total_errors"] == 0
        assert stats["buffer_size"] == 0

    def test_flush_to_postgres_writer(self):
        handler = DLQHandler(DLQConfig(enabled=True))
        mock_writer = MagicMock()
        mock_writer.write_dlq.return_value = True
        handler.set_writer(mock_writer)

        handler.route_to_dlq("topic1", "ERROR1", "msg1", {"k": "v1"})
        handler.route_to_dlq("topic2", "ERROR2", "msg2", {"k": "v2"})

        flushed = handler.flush()
        assert flushed == 2
        assert mock_writer.write_dlq.call_count == 2
        assert handler.get_stats()["buffer_size"] == 0

    def test_flush_no_writer(self):
        handler = DLQHandler(DLQConfig(enabled=True))
        handler.route_to_dlq("topic", "ERROR", "msg", {})

        flushed = handler.flush()
        assert flushed == 0

    def test_flush_empty_buffer(self):
        handler = DLQHandler(DLQConfig(enabled=True))
        flushed = handler.flush()
        assert flushed == 0

    def test_auto_flush_at_100(self):
        handler = DLQHandler(DLQConfig(enabled=True))
        mock_writer = MagicMock()
        mock_writer.write_dlq.return_value = True
        handler.set_writer(mock_writer)

        for i in range(100):
            handler.route_to_dlq(f"topic_{i}", "ERROR", "msg", {})

        # Auto-flush should have been triggered
        assert handler.get_stats()["buffer_size"] == 0
        assert mock_writer.write_dlq.call_count == 100

    def test_flush_handles_writer_error(self):
        handler = DLQHandler(DLQConfig(enabled=True))
        mock_writer = MagicMock()
        mock_writer.write_dlq.side_effect = Exception("DB error")
        handler.set_writer(mock_writer)

        handler.route_to_dlq("topic", "ERROR", "msg", {})
        flushed = handler.flush()

        assert flushed == 0
        assert handler.get_stats()["buffer_size"] == 0  # Buffer cleared even on error

    def test_error_type_constants(self):
        assert DLQHandler.SCHEMA_MISMATCH == "SCHEMA_MISMATCH"
        assert DLQHandler.MALFORMED_JSON == "MALFORMED_JSON"
        assert DLQHandler.MALFORMED_XML == "MALFORMED_XML"
        assert DLQHandler.PROCESSING_ERROR == "PROCESSING_ERROR"
        assert DLQHandler.WRITE_ERROR == "WRITE_ERROR"

    def test_stats_tracking(self):
        handler = DLQHandler(DLQConfig(enabled=True))

        handler.route_to_dlq("t1", "E1", "m1", {})
        handler.route_to_dlq("t2", "E2", "m2", {})

        stats = handler.get_stats()
        assert stats["total_errors"] == 2
        assert stats["buffer_size"] == 2
        assert stats["enabled"] is True
