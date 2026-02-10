"""
CDC Data Generator Main Entry Point
Orchestrates event generation and publishing to Solace
"""

import logging
import random
import signal
import sys
import time
from typing import Dict, Any

from .config import get_config, Config, TableDistribution
from .debezium_simulator import DebeziumEventBuilder, Operation
from .publisher import SolacePublisher
from .models import (
    OrderGenerator,
    OrderItemGenerator,
    CustomerGenerator,
    ProductGenerator,
    AuditLogGenerator,
    BaseTableGenerator
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class CDCGenerator:
    """Main CDC event generator orchestrator"""

    def __init__(self, config: Config):
        self.config = config
        self.running = False

        # Initialize components
        self.publisher = SolacePublisher(config.solace)
        self.event_builder = DebeziumEventBuilder(
            connector=config.generator.debezium_connector,
            db_name=config.generator.debezium_db_name,
            schema_name=config.generator.debezium_schema,
            version=config.generator.debezium_version
        )

        # Initialize table generators
        self.generators: Dict[str, BaseTableGenerator] = {
            "ORDERS": OrderGenerator(
                xml_enabled=config.generator.xml_enabled,
                xml_probability=config.generator.xml_probability,
                schema_evolution=config.generator.schema_evolution,
                evolution_probability=config.generator.evolution_probability,
            ),
            "ORDER_ITEMS": OrderItemGenerator(),
            "CUSTOMERS": CustomerGenerator(),
            "PRODUCTS": ProductGenerator(),
            "AUDIT_LOG": AuditLogGenerator()
        }

        # Link order items to orders
        self._order_item_gen: OrderItemGenerator = self.generators["ORDER_ITEMS"]

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def _select_table(self) -> str:
        """Select table based on distribution weights"""
        distributions = self.config.generator.table_distributions
        tables = list(distributions.keys())
        weights = [d.percentage for d in distributions.values()]
        return random.choices(tables, weights=weights)[0]

    def _select_operation(self, table: str) -> str:
        """Select operation based on table distribution"""
        dist = self.config.generator.table_distributions.get(table)
        if not dist:
            return "insert"

        ops = list(dist.operations.keys())
        weights = list(dist.operations.values())
        return random.choices(ops, weights=weights)[0]

    def _generate_event(self, table: str, operation: str) -> tuple[Any, Dict[str, Any] | None]:
        """Generate event for table and operation"""
        generator = self.generators.get(table)
        if not generator:
            logger.warning(f"No generator for table: {table}")
            return None, None

        if operation == "insert":
            data = generator.generate_insert()

            # Track new order IDs for order items
            if table == "ORDERS" and data:
                self._order_item_gen.add_order_id(data.get("ORDER_ID", 0))

            if data:
                event = self.event_builder.create_insert(table, data)
                return event, None
            return None, None

        elif operation == "update":
            result = generator.generate_update()
            if result:
                before, after = result
                event = self.event_builder.create_update(table, before, after)
                return event, None
            return None, None

        elif operation == "delete":
            data = generator.generate_delete()
            if data:
                event = self.event_builder.create_delete(table, data)
                return event, None
            return None, None

        return None, None

    def run(self) -> None:
        """Main generator loop"""
        logger.info("=" * 60)
        logger.info("CDC Data Generator Starting")
        logger.info("=" * 60)

        # Connect to Solace
        if not self.publisher.connect():
            logger.error("Failed to connect to Solace, exiting")
            sys.exit(1)

        self.running = True
        events_per_second = self.config.generator.get_events_per_second()
        interval = 1.0 / events_per_second if events_per_second > 0 else 0.01

        logger.info(f"Target rate: {self.config.generator.events_per_minute} events/minute")
        logger.info(f"Events per second: {events_per_second:.2f}")
        logger.info(f"Interval: {interval*1000:.2f}ms")
        logger.info("-" * 60)

        last_stats_time = time.time()
        stats_interval = 10  # Print stats every 10 seconds

        try:
            while self.running:
                loop_start = time.time()

                # Select table and operation
                table = self._select_table()
                operation = self._select_operation(table)

                # Generate and publish event
                event, _ = self._generate_event(table, operation)
                if event:
                    self.publisher.publish(event, table, operation, schema=self.config.generator.debezium_schema)

                # Print periodic stats
                if time.time() - last_stats_time >= stats_interval:
                    stats = self.publisher.get_stats()
                    logger.info(
                        f"Stats: {stats['messages_sent']} sent, "
                        f"{stats['messages_failed']} failed, "
                        f"{stats['rate_per_second']:.1f} msg/sec"
                    )
                    last_stats_time = time.time()

                # Rate limiting
                elapsed = time.time() - loop_start
                sleep_time = max(0, interval - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")

        finally:
            self._shutdown()

    def _shutdown(self) -> None:
        """Cleanup on shutdown"""
        logger.info("-" * 60)
        logger.info("Shutting down generator...")

        # Final stats
        stats = self.publisher.get_stats()
        logger.info(f"Final Stats:")
        logger.info(f"  Messages sent: {stats['messages_sent']}")
        logger.info(f"  Messages failed: {stats['messages_failed']}")
        logger.info(f"  Bytes sent: {stats['bytes_sent']}")
        logger.info(f"  Duration: {stats['elapsed_seconds']}s")
        logger.info(f"  Average rate: {stats['rate_per_second']:.1f} msg/sec")

        # Disconnect
        self.publisher.disconnect()
        logger.info("Generator stopped")


def main():
    """Entry point"""
    # Load configuration
    config = get_config()

    # Set log level
    logging.getLogger().setLevel(config.log_level)

    # Create and run generator
    generator = CDCGenerator(config)
    generator.run()


if __name__ == "__main__":
    main()
