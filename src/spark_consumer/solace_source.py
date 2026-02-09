"""
Solace PubSub+ Source Connector for PySpark
Consumes CDC events from Solace and feeds them into Spark processing pipeline
"""

import json
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver import MessageHandler
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError

from .config import SolaceSourceConfig

logger = logging.getLogger(__name__)


@dataclass
class CDCMessage:
    """Parsed CDC message from Solace"""
    topic: str
    table: str
    operation: str
    payload: Dict[str, Any]
    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    source_ts: int  # milliseconds
    received_at: float = field(default_factory=time.time)

    @property
    def source_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.source_ts / 1000)


class SolaceMessageHandler(MessageHandler):
    """Handler for incoming Solace messages"""

    def __init__(self, message_queue: deque, max_queue_size: int = 50000):
        self._queue = message_queue
        self._max_queue_size = max_queue_size
        self._received_count = 0
        self._dropped_count = 0

    def on_message(self, message):
        """Handle incoming message"""
        try:
            topic = message.get_destination_name()
            body = message.get_payload_as_string()

            if not body:
                logger.warning(f"Empty message received on {topic}")
                return

            # Parse the Debezium event
            event = json.loads(body)
            payload = event.get("payload", {})

            # Extract table name from topic: cdc/oracle/SALES/ORDERS/insert
            parts = topic.split("/")
            table = parts[3] if len(parts) > 3 else "UNKNOWN"
            operation = parts[4] if len(parts) > 4 else "unknown"

            cdc_msg = CDCMessage(
                topic=topic,
                table=table,
                operation=operation,
                payload=payload,
                before=payload.get("before"),
                after=payload.get("after"),
                source_ts=payload.get("ts_ms", int(time.time() * 1000))
            )

            # Backpressure: drop if queue is full
            if len(self._queue) >= self._max_queue_size:
                self._dropped_count += 1
                if self._dropped_count % 100 == 0:
                    logger.warning(
                        f"Queue full, dropped {self._dropped_count} messages. "
                        f"Queue size: {len(self._queue)}"
                    )
                return

            self._queue.append(cdc_msg)
            self._received_count += 1

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")


class SolaceSource:
    """Solace source connector for PySpark consumer"""

    def __init__(self, config: SolaceSourceConfig):
        self.config = config
        self._messaging_service: Optional[MessagingService] = None
        self._receiver = None
        self._connected = False
        self._message_queue: deque = deque()
        self._handler: Optional[SolaceMessageHandler] = None

        # Stats
        self._connect_time: Optional[float] = None

    def connect(self) -> bool:
        """Connect to Solace and start receiving messages"""
        try:
            broker_props = {
                "solace.messaging.transport.host": f"tcp://{self.config.host}:{self.config.port}",
                "solace.messaging.service.vpn-name": self.config.vpn,
                "solace.messaging.authentication.scheme.basic.username": self.config.username,
                "solace.messaging.authentication.scheme.basic.password": self.config.password,
                "solace.messaging.transport.connection-retries": self.config.reconnect_retries,
                "solace.messaging.transport.reconnection-attempts": self.config.reconnect_retries,
                "solace.messaging.transport.reconnection-attempts-wait-interval": self.config.reconnect_retry_wait_ms,
                "solace.messaging.transport.connection-retries-per-host": 3,
            }

            self._messaging_service = MessagingService.builder() \
                .from_properties(broker_props) \
                .build()

            self._messaging_service.connect()
            logger.info(f"Connected to Solace at {self.config.host}:{self.config.port}")

            # Create topic subscriptions
            subscriptions = [
                TopicSubscription.of(topic) for topic in self.config.topics
            ]

            # Create direct message receiver
            self._receiver = self._messaging_service \
                .create_direct_message_receiver_builder() \
                .with_subscriptions(subscriptions) \
                .build()

            # Set message handler
            self._handler = SolaceMessageHandler(self._message_queue)
            self._receiver.start()
            self._receiver.receive_async(self._handler)

            self._connected = True
            self._connect_time = time.time()

            logger.info(f"Subscribed to {len(subscriptions)} topics")
            for topic in self.config.topics:
                logger.info(f"  - {topic}")

            return True

        except PubSubPlusClientError as e:
            logger.error(f"Failed to connect to Solace: {e}")
            return False

    def disconnect(self) -> None:
        """Disconnect from Solace"""
        try:
            if self._receiver:
                self._receiver.terminate()
            if self._messaging_service:
                self._messaging_service.disconnect()
            self._connected = False
            logger.info("Disconnected from Solace")
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")

    def poll_messages(self, max_batch: int = 1000) -> List[CDCMessage]:
        """Poll messages from internal queue (non-blocking)"""
        messages = []
        while self._message_queue and len(messages) < max_batch:
            try:
                msg = self._message_queue.popleft()
                messages.append(msg)
            except IndexError:
                break
        return messages

    def get_messages_by_table(self, max_batch: int = 1000) -> Dict[str, List[CDCMessage]]:
        """Poll messages grouped by table"""
        messages = self.poll_messages(max_batch)
        by_table: Dict[str, List[CDCMessage]] = {}
        for msg in messages:
            by_table.setdefault(msg.table, []).append(msg)
        return by_table

    def queue_size(self) -> int:
        """Current queue depth"""
        return len(self._message_queue)

    def is_connected(self) -> bool:
        return self._connected

    def get_stats(self) -> Dict[str, Any]:
        """Get source statistics"""
        stats = {
            "connected": self._connected,
            "queue_depth": len(self._message_queue),
        }
        if self._handler:
            stats["received_count"] = self._handler._received_count
            stats["dropped_count"] = self._handler._dropped_count
        if self._connect_time:
            stats["uptime_seconds"] = round(time.time() - self._connect_time, 2)
        return stats

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
