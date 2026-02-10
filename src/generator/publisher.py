"""
Solace PubSub+ Publisher
Handles publishing CDC events to Solace message broker
"""

import logging
import time
from typing import Optional
from dataclasses import dataclass

from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError

from .config import SolaceConfig
from .debezium_simulator import DebeziumEvent

logger = logging.getLogger(__name__)


class PublishFailureHandler(PublishFailureListener):
    """Handle publish failures"""

    def on_failed_publish(self, failed_publish_event):
        logger.error(
            f"Publish failed: {failed_publish_event.get_exception()} "
            f"for topic: {failed_publish_event.get_destination()}"
        )


@dataclass
class PublishStats:
    """Track publishing statistics"""
    messages_sent: int = 0
    messages_failed: int = 0
    bytes_sent: int = 0
    start_time: float = 0.0

    def __post_init__(self):
        if not self.start_time:
            self.start_time = time.time()

    def record_success(self, message_size: int) -> None:
        self.messages_sent += 1
        self.bytes_sent += message_size

    def record_failure(self) -> None:
        self.messages_failed += 1

    def get_rate(self) -> float:
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            return self.messages_sent / elapsed
        return 0.0

    def get_summary(self) -> dict:
        elapsed = time.time() - self.start_time
        return {
            "messages_sent": self.messages_sent,
            "messages_failed": self.messages_failed,
            "bytes_sent": self.bytes_sent,
            "elapsed_seconds": round(elapsed, 2),
            "rate_per_second": round(self.get_rate(), 2)
        }


class SolacePublisher:
    """Solace PubSub+ event publisher"""

    def __init__(self, config: SolaceConfig):
        self.config = config
        self._messaging_service: Optional[MessagingService] = None
        self._publisher = None
        self._connected = False
        self.stats = PublishStats()

    def connect(self) -> bool:
        """Connect to Solace broker"""
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

            # Create direct publisher
            self._publisher = self._messaging_service.create_direct_message_publisher_builder() \
                .on_back_pressure_reject(buffer_capacity=50000) \
                .build()

            self._publisher.set_publish_failure_listener(PublishFailureHandler())
            self._publisher.start()

            self._connected = True
            logger.info("Solace publisher started")
            return True

        except PubSubPlusClientError as e:
            logger.error(f"Failed to connect to Solace: {e}")
            return False

    def disconnect(self) -> None:
        """Disconnect from Solace broker"""
        try:
            if self._publisher:
                self._publisher.terminate()
                logger.info("Publisher terminated")

            if self._messaging_service:
                self._messaging_service.disconnect()
                logger.info("Disconnected from Solace")

            self._connected = False

        except Exception as e:
            logger.error(f"Error during disconnect: {e}")

    def publish(
        self,
        event: DebeziumEvent,
        table: str,
        operation: str,
        schema: str = "SALES"
    ) -> bool:
        """Publish CDC event to Solace topic"""
        if not self._connected:
            logger.error("Not connected to Solace")
            return False

        try:
            # Build topic name: cdc/oracle/SALES/ORDERS/insert
            topic_name = self.config.get_topic(
                schema,
                table,
                operation
            )

            # Create topic and message
            topic = Topic.of(topic_name)
            message_body = event.to_json()

            # Create outbound message
            message = self._messaging_service.message_builder() \
                .with_application_message_id(f"{table}-{time.time_ns()}") \
                .build(message_body)

            # Publish
            self._publisher.publish(message, topic)

            self.stats.record_success(len(message_body))

            logger.debug(f"Published to {topic_name}: {len(message_body)} bytes")
            return True

        except PubSubPlusClientError as e:
            logger.error(f"Publish error: {e}")
            self.stats.record_failure()
            return False

    def publish_batch(
        self,
        events: list[tuple[DebeziumEvent, str, str]]
    ) -> int:
        """Publish batch of events. Returns number of successful publishes."""
        success_count = 0
        for event, table, operation in events:
            if self.publish(event, table, operation):
                success_count += 1
        return success_count

    def is_connected(self) -> bool:
        """Check if connected to Solace"""
        return self._connected

    def get_stats(self) -> dict:
        """Get publishing statistics"""
        return self.stats.get_summary()

    def reset_stats(self) -> None:
        """Reset statistics"""
        self.stats = PublishStats()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
