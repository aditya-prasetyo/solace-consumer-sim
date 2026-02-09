"""
Dead Letter Queue Handler for PySpark CDC Consumer
Routes failed messages to DLQ topic and/or PostgreSQL DLQ table
"""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from .config import DLQConfig

logger = logging.getLogger(__name__)


class DLQRecord:
    """Represents a failed message for DLQ"""

    def __init__(
        self,
        original_topic: str,
        error_type: str,
        error_message: str,
        payload: Dict[str, Any],
        consumer_id: str = "spark-consumer-1"
    ):
        self.original_topic = original_topic
        self.error_type = error_type
        self.error_message = error_message
        self.payload = payload
        self.consumer_id = consumer_id
        self.retry_count = 0
        self.first_failure_at = datetime.now()
        self.last_failure_at = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage"""
        return {
            "original_topic": self.original_topic,
            "error_type": self.error_type,
            "error_message": self.error_message[:1000],
            "retry_count": self.retry_count,
            "payload": json.dumps(self.payload) if isinstance(self.payload, dict) else str(self.payload),
            "first_failure_at": self.first_failure_at,
            "last_failure_at": self.last_failure_at,
            "consumer_id": self.consumer_id,
            "status": "pending"
        }


class DLQHandler:
    """Handle failed messages by routing to Dead Letter Queue"""

    # Error type constants
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    MALFORMED_JSON = "MALFORMED_JSON"
    MALFORMED_XML = "MALFORMED_XML"
    CONSTRAINT_VIOLATION = "CONSTRAINT_VIOLATION"
    PROCESSING_ERROR = "PROCESSING_ERROR"
    TRANSFORMATION_ERROR = "TRANSFORMATION_ERROR"
    WRITE_ERROR = "WRITE_ERROR"

    def __init__(self, config: DLQConfig, consumer_id: str = "spark-consumer-1"):
        self.config = config
        self.consumer_id = consumer_id
        self._buffer: List[DLQRecord] = []
        self._total_count = 0
        self._postgres_writer = None  # Set externally

    def set_writer(self, writer) -> None:
        """Set PostgreSQL writer for DLQ persistence"""
        self._postgres_writer = writer

    def route_to_dlq(
        self,
        original_topic: str,
        error_type: str,
        error_message: str,
        payload: Dict[str, Any]
    ) -> None:
        """Route a failed message to DLQ"""
        if not self.config.enabled:
            logger.warning(f"DLQ disabled, discarding error: {error_type}")
            return

        record = DLQRecord(
            original_topic=original_topic,
            error_type=error_type,
            error_message=error_message,
            payload=payload,
            consumer_id=self.consumer_id
        )

        self._buffer.append(record)
        self._total_count += 1

        logger.warning(
            f"DLQ: {error_type} for topic {original_topic}: {error_message[:100]}"
        )

        # Auto-flush if buffer is large
        if len(self._buffer) >= 100:
            self.flush()

    def flush(self) -> int:
        """Flush buffered DLQ records to PostgreSQL"""
        if not self._buffer:
            return 0

        if not self._postgres_writer:
            logger.error("No PostgreSQL writer set for DLQ, cannot flush")
            return 0

        flushed = 0
        for record in self._buffer:
            try:
                self._postgres_writer.write_dlq(
                    original_topic=record.original_topic,
                    error_type=record.error_type,
                    error_message=record.error_message,
                    payload=record.payload,
                    consumer_id=record.consumer_id
                )
                flushed += 1
            except Exception as e:
                logger.error(f"Failed to flush DLQ record: {e}")

        self._buffer.clear()
        if flushed > 0:
            logger.info(f"Flushed {flushed} DLQ records to PostgreSQL")
        return flushed

    def get_stats(self) -> Dict[str, Any]:
        """Get DLQ statistics"""
        return {
            "total_errors": self._total_count,
            "buffer_size": len(self._buffer),
            "enabled": self.config.enabled
        }
