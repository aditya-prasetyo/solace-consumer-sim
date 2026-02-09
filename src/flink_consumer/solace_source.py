"""
Solace PubSub+ Source for PyFlink Consumer
Consumes CDC events from Solace and feeds them into Flink DataStream pipeline
"""

import json
import logging
import time
from collections import deque
from typing import Any, Dict, List, Optional

from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_handler import MessageHandler
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError

from .config import SolaceSourceConfig

logger = logging.getLogger(__name__)


class CDCRecord:
    """Parsed CDC record from Solace"""

    __slots__ = ("topic", "table", "operation", "before", "after", "source_ts", "scn", "payload")

    def __init__(
        self, topic: str, table: str, operation: str,
        before: Optional[Dict], after: Optional[Dict],
        source_ts: int, scn: str = "", payload: Optional[Dict] = None
    ):
        self.topic = topic
        self.table = table
        self.operation = operation
        self.before = before
        self.after = after
        self.source_ts = source_ts
        self.scn = scn
        self.payload = payload or {}

    @property
    def data(self) -> Dict[str, Any]:
        """Get the relevant data based on operation"""
        if self.operation == "d":
            return self.before or {}
        return self.after or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "topic": self.topic,
            "table": self.table,
            "operation": self.operation,
            "before": self.before,
            "after": self.after,
            "source_ts": self.source_ts,
            "scn": self.scn,
        }


class _MessageHandler(MessageHandler):
    """Internal Solace message handler"""

    def __init__(self, queue: deque, max_size: int = 50000):
        self._queue = queue
        self._max_size = max_size
        self.received = 0
        self.dropped = 0

    def on_message(self, message):
        try:
            topic = message.get_destination_name()
            body = message.get_payload_as_string()
            if not body:
                return

            event = json.loads(body)
            payload = event.get("payload", {})
            source = payload.get("source", {})

            parts = topic.split("/")
            table = parts[3] if len(parts) > 3 else "UNKNOWN"
            operation = parts[4] if len(parts) > 4 else "unknown"

            record = CDCRecord(
                topic=topic,
                table=table,
                operation=operation,
                before=payload.get("before"),
                after=payload.get("after"),
                source_ts=payload.get("ts_ms", int(time.time() * 1000)),
                scn=source.get("scn", ""),
                payload=payload,
            )

            if len(self._queue) >= self._max_size:
                self.dropped += 1
                return

            self._queue.append(record)
            self.received += 1

        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
        except Exception as e:
            logger.error(f"Message handler error: {e}")


class FlinkSolaceSource:
    """Solace source for PyFlink DataStream"""

    def __init__(self, config: SolaceSourceConfig):
        self.config = config
        self._service: Optional[MessagingService] = None
        self._receiver = None
        self._handler: Optional[_MessageHandler] = None
        self._queue: deque = deque()
        self._connected = False

    def connect(self) -> bool:
        try:
            props = {
                "solace.messaging.transport.host": f"tcp://{self.config.host}:{self.config.port}",
                "solace.messaging.service.vpn-name": self.config.vpn,
                "solace.messaging.authentication.scheme.basic.username": self.config.username,
                "solace.messaging.authentication.scheme.basic.password": self.config.password,
                "solace.messaging.transport.connection-retries": self.config.reconnect_retries,
                "solace.messaging.transport.reconnection-attempts": self.config.reconnect_retries,
                "solace.messaging.transport.reconnection-attempts-wait-interval": self.config.reconnect_retry_wait_ms,
            }

            self._service = MessagingService.builder().from_properties(props).build()
            self._service.connect()

            subscriptions = [TopicSubscription.of(t) for t in self.config.topics]
            self._receiver = self._service.create_direct_message_receiver_builder() \
                .with_subscriptions(subscriptions).build()

            self._handler = _MessageHandler(self._queue)
            self._receiver.start()
            self._receiver.receive_async(self._handler)

            self._connected = True
            logger.info(f"Connected to Solace at {self.config.host}:{self.config.port}")
            return True

        except PubSubPlusClientError as e:
            logger.error(f"Solace connection failed: {e}")
            return False

    def disconnect(self) -> None:
        try:
            if self._receiver:
                self._receiver.terminate()
            if self._service:
                self._service.disconnect()
            self._connected = False
            logger.info("Disconnected from Solace")
        except Exception as e:
            logger.error(f"Disconnect error: {e}")

    def poll(self, max_batch: int = 500) -> List[CDCRecord]:
        """Poll records from queue"""
        records = []
        while self._queue and len(records) < max_batch:
            try:
                records.append(self._queue.popleft())
            except IndexError:
                break
        return records

    def poll_by_table(self, max_batch: int = 500) -> Dict[str, List[CDCRecord]]:
        """Poll records grouped by table"""
        records = self.poll(max_batch)
        by_table: Dict[str, List[CDCRecord]] = {}
        for r in records:
            by_table.setdefault(r.table, []).append(r)
        return by_table

    def queue_size(self) -> int:
        return len(self._queue)

    def is_connected(self) -> bool:
        return self._connected

    def get_stats(self) -> Dict[str, Any]:
        stats = {"connected": self._connected, "queue_depth": len(self._queue)}
        if self._handler:
            stats["received"] = self._handler.received
            stats["dropped"] = self._handler.dropped
        return stats

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()
