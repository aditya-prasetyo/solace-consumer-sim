"""
Generator Configuration Module
Environment-aware configuration for the Debezium CDC Simulator
"""

import os
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class SolaceConfig:
    """Solace connection configuration"""
    host: str = field(default_factory=lambda: os.getenv("SOLACE_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("SOLACE_PORT", "55555")))
    vpn: str = field(default_factory=lambda: os.getenv("SOLACE_VPN", "default"))
    username: str = field(default_factory=lambda: os.getenv("SOLACE_USERNAME", "admin"))
    password: str = field(default_factory=lambda: os.getenv("SOLACE_PASSWORD", "admin"))

    # Connection settings
    connect_timeout_ms: int = 30000
    reconnect_retries: int = -1  # Infinite
    reconnect_retry_wait_ms: int = 3000

    # Topic pattern
    topic_prefix: str = "cdc/oracle"

    def get_topic(self, schema: str, table: str, operation: str) -> str:
        """Generate topic name from pattern"""
        return f"{self.topic_prefix}/{schema}/{table}/{operation}"


@dataclass
class TableDistribution:
    """Event distribution per table"""
    name: str
    events_per_minute: int
    percentage: float
    operations: Dict[str, int]  # operation -> percentage


@dataclass
class GeneratorConfig:
    """Main generator configuration"""

    # Event generation rate
    events_per_minute: int = field(
        default_factory=lambda: int(os.getenv("EVENTS_PER_MINUTE", "5000"))
    )

    # Debezium settings
    debezium_connector: str = "oracle"
    debezium_db_name: str = "ORCL"
    debezium_schema: str = "SALES"
    debezium_version: str = "2.4.0"

    # XML generation
    xml_enabled: bool = True
    xml_probability: float = 0.3  # 30% of orders have XML shipping info
    xml_max_items: int = 5

    # Batch settings
    batch_size: int = 100
    batch_interval_ms: int = 1000

    # Table distributions (based on blueprint)
    @property
    def table_distributions(self) -> Dict[str, TableDistribution]:
        """Get table distribution configuration"""
        total = self.events_per_minute
        return {
            "ORDERS": TableDistribution(
                name="ORDERS",
                events_per_minute=int(total * 0.40),
                percentage=40.0,
                operations={"insert": 70, "update": 25, "delete": 5}
            ),
            "ORDER_ITEMS": TableDistribution(
                name="ORDER_ITEMS",
                events_per_minute=int(total * 0.40),
                percentage=40.0,
                operations={"insert": 80, "update": 15, "delete": 5}
            ),
            "CUSTOMERS": TableDistribution(
                name="CUSTOMERS",
                events_per_minute=int(total * 0.10),
                percentage=10.0,
                operations={"insert": 20, "update": 75, "delete": 5}
            ),
            "PRODUCTS": TableDistribution(
                name="PRODUCTS",
                events_per_minute=int(total * 0.08),
                percentage=8.0,
                operations={"insert": 10, "update": 85, "delete": 5}
            ),
            "AUDIT_LOG": TableDistribution(
                name="AUDIT_LOG",
                events_per_minute=int(total * 0.02),
                percentage=2.0,
                operations={"insert": 100, "update": 0, "delete": 0}
            ),
        }

    def get_events_per_second(self) -> float:
        """Calculate events per second"""
        return self.events_per_minute / 60.0


@dataclass
class Config:
    """Root configuration"""
    solace: SolaceConfig = field(default_factory=SolaceConfig)
    generator: GeneratorConfig = field(default_factory=GeneratorConfig)
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))


# Global config instance
_config: Config | None = None


def get_config() -> Config:
    """Get or create global config instance"""
    global _config
    if _config is None:
        _config = Config()
    return _config


def reload_config() -> Config:
    """Force reload configuration from environment"""
    global _config
    _config = Config()
    return _config
