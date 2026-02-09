"""
Dead Letter Queue Handler for PyFlink CDC Consumer
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from .config import DLQConfig

logger = logging.getLogger(__name__)


class FlinkDLQHandler:
    """Route failed records to DLQ via PostgreSQL and/or REST API"""

    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    MALFORMED_JSON = "MALFORMED_JSON"
    MALFORMED_XML = "MALFORMED_XML"
    PROCESSING_ERROR = "PROCESSING_ERROR"
    WRITE_ERROR = "WRITE_ERROR"

    def __init__(self, config: DLQConfig, consumer_id: str = "flink-consumer-1"):
        self.config = config
        self.consumer_id = consumer_id
        self._buffer: List[Dict[str, Any]] = []
        self._total = 0
        self._pg_sink = None
        self._rest_sink = None

    def set_pg_sink(self, sink) -> None:
        self._pg_sink = sink

    def set_rest_sink(self, sink) -> None:
        self._rest_sink = sink

    def route(
        self, topic: str, error_type: str, error_msg: str, payload: Dict[str, Any]
    ) -> None:
        if not self.config.enabled:
            return

        self._buffer.append({
            "topic": topic,
            "error_type": error_type,
            "error_msg": error_msg,
            "payload": payload,
        })
        self._total += 1

        logger.warning(f"DLQ: {error_type} for {topic}: {error_msg[:100]}")

        if len(self._buffer) >= 50:
            self.flush()

    def flush(self) -> int:
        if not self._buffer:
            return 0

        flushed = 0
        for item in self._buffer:
            # Try PostgreSQL first
            if self._pg_sink:
                try:
                    self._pg_sink.write_dlq(
                        topic=item["topic"],
                        error_type=item["error_type"],
                        error_msg=item["error_msg"],
                        payload=item["payload"],
                        consumer_id=self.consumer_id,
                    )
                    flushed += 1
                except Exception as e:
                    logger.error(f"DLQ PG flush failed: {e}")

            # Also notify via REST API
            if self._rest_sink:
                try:
                    self._rest_sink.send_notification(
                        event_type="DLQ_ENTRY",
                        message=f"{item['error_type']}: {item['error_msg'][:200]}",
                        severity="error",
                        payload={"topic": item["topic"]},
                    )
                except Exception:
                    pass

        self._buffer.clear()
        return flushed

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_errors": self._total,
            "buffer_size": len(self._buffer),
            "enabled": self.config.enabled,
        }
