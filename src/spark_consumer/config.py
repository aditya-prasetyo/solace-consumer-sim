"""
PySpark Consumer Configuration
Loads and manages all configuration for Architecture A
"""

import os
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "configs")


def _resolve_env(value: str) -> str:
    """Resolve ${ENV_VAR:default} patterns in config values"""
    if not isinstance(value, str) or "${" not in value:
        return value
    import re
    pattern = r"\$\{([^:}]+)(?::([^}]*))?\}"
    def replacer(match):
        env_var = match.group(1)
        default = match.group(2) or ""
        return os.environ.get(env_var, default)
    return re.sub(pattern, replacer, value)


def _resolve_dict(d: dict) -> dict:
    """Recursively resolve environment variables in config dict"""
    result = {}
    for k, v in d.items():
        if isinstance(v, dict):
            result[k] = _resolve_dict(v)
        elif isinstance(v, list):
            result[k] = [_resolve_env(i) if isinstance(i, str) else i for i in v]
        elif isinstance(v, str):
            result[k] = _resolve_env(v)
        else:
            result[k] = v
    return result


@dataclass
class SparkConfig:
    """Spark application configuration"""
    app_name: str = "CDC-Spark-Consumer"
    master: str = "local[*]"
    driver_memory: str = "2g"
    executor_memory: str = "1g"
    executor_cores: int = 2
    max_result_size: str = "1g"
    shuffle_partitions: int = 4
    ui_port: int = 4040


@dataclass
class StreamingConfig:
    """Streaming processing configuration"""
    trigger_interval: str = "10 seconds"
    checkpoint_location: str = "/checkpoints"
    watermark_delay: str = "10 seconds"
    output_mode: str = "append"


@dataclass
class SolaceSourceConfig:
    """Solace connection configuration for consumer"""
    host: str = "localhost"
    port: int = 55555
    vpn: str = "default"
    username: str = "admin"
    password: str = "admin"
    topics: List[str] = field(default_factory=lambda: [
        "cdc/oracle/SALES/ORDERS/*",
        "cdc/oracle/SALES/ORDER_ITEMS/*",
        "cdc/oracle/SALES/CUSTOMERS/*",
        "cdc/oracle/SALES/PRODUCTS/*",
        "cdc/oracle/SALES/AUDIT_LOG/*"
    ])
    queue: str = "cdc-spark-consumer"
    reconnect_retries: int = -1
    reconnect_retry_wait_ms: int = 3000
    connect_timeout_ms: int = 30000
    keepalive_interval_ms: int = 3000


@dataclass
class PostgresConfig:
    """PostgreSQL ODS configuration"""
    host: str = "localhost"
    port: int = 5432
    database: str = "ods"
    username: str = "cdc_user"
    password: str = "cdc_pass"
    pool_min: int = 2
    pool_max: int = 10
    pool_overflow: int = 5
    pool_timeout: int = 30
    pool_recycle: int = 1800
    batch_size: int = 1000
    batch_timeout: int = 5

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def connection_url(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class JoinConfig:
    """3-way join configuration"""
    window_duration: str = "5 minutes"
    state_ttl: str = "1 hour"


@dataclass
class XMLConfig:
    """XML extraction configuration"""
    enabled: bool = True
    column: str = "shipping_info"
    target_table: str = "cdc_order_items_extracted"


@dataclass
class RetryConfig:
    """Retry policy configuration"""
    max_attempts: int = 3
    initial_delay_ms: int = 1000
    max_delay_ms: int = 30000
    multiplier: float = 2.0


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration"""
    failure_threshold: int = 5
    recovery_timeout: int = 30


@dataclass
class DLQConfig:
    """Dead Letter Queue configuration"""
    enabled: bool = True
    topic_prefix: str = "cdc/dlq"


@dataclass
class ErrorHandlingConfig:
    """Error handling configuration"""
    retry: RetryConfig = field(default_factory=RetryConfig)
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    dlq: DLQConfig = field(default_factory=DLQConfig)


@dataclass
class RestApiSinkConfig:
    """REST API sink configuration"""
    enabled: bool = True
    base_url: str = "http://localhost:8000"
    timeout: int = 10
    consumer_id: str = "spark-consumer-1"


@dataclass
class SparkConsumerConfig:
    """Root configuration for PySpark Consumer"""
    spark: SparkConfig = field(default_factory=SparkConfig)
    streaming: StreamingConfig = field(default_factory=StreamingConfig)
    solace: SolaceSourceConfig = field(default_factory=SolaceSourceConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    rest_api: RestApiSinkConfig = field(default_factory=RestApiSinkConfig)
    join: JoinConfig = field(default_factory=JoinConfig)
    xml: XMLConfig = field(default_factory=XMLConfig)
    error_handling: ErrorHandlingConfig = field(default_factory=ErrorHandlingConfig)
    log_level: str = "INFO"


def _load_yaml(filename: str) -> dict:
    """Load YAML config file"""
    filepath = os.path.join(CONFIG_DIR, filename)
    if not os.path.exists(filepath):
        logger.warning(f"Config file not found: {filepath}")
        return {}
    with open(filepath, "r") as f:
        return yaml.safe_load(f) or {}


def load_config() -> SparkConsumerConfig:
    """Load configuration from YAML files and environment variables"""
    # Load YAML files
    spark_raw = _resolve_dict(_load_yaml("spark.yaml"))
    postgres_raw = _resolve_dict(_load_yaml("postgres.yaml"))

    config = SparkConsumerConfig()

    # Parse spark section
    spark_section = spark_raw.get("spark", {})
    config.spark.app_name = spark_section.get("app_name", config.spark.app_name)
    config.spark.master = spark_section.get("master", config.spark.master)

    driver = spark_section.get("driver", {})
    config.spark.driver_memory = driver.get("memory", config.spark.driver_memory)
    config.spark.max_result_size = driver.get("max_result_size", config.spark.max_result_size)

    executor = spark_section.get("executor", {})
    config.spark.executor_memory = executor.get("memory", config.spark.executor_memory)
    config.spark.executor_cores = executor.get("cores", config.spark.executor_cores)

    shuffle = spark_section.get("shuffle", {})
    config.spark.shuffle_partitions = shuffle.get("partitions", config.spark.shuffle_partitions)

    # Parse streaming section
    streaming = spark_section.get("streaming", {})
    config.streaming.trigger_interval = streaming.get("trigger_interval", config.streaming.trigger_interval)
    config.streaming.checkpoint_location = streaming.get("checkpoint_location", config.streaming.checkpoint_location)
    config.streaming.watermark_delay = streaming.get("watermark_delay", config.streaming.watermark_delay)
    config.streaming.output_mode = streaming.get("output_mode", config.streaming.output_mode)

    # Parse solace section
    solace_section = spark_raw.get("solace", {})
    config.solace.host = solace_section.get("host", config.solace.host)
    config.solace.port = int(solace_section.get("port", config.solace.port))
    config.solace.vpn = solace_section.get("vpn", config.solace.vpn)
    config.solace.username = solace_section.get("username", config.solace.username)
    config.solace.password = solace_section.get("password", config.solace.password)
    config.solace.topics = solace_section.get("topics", config.solace.topics)
    config.solace.queue = solace_section.get("queue", config.solace.queue)

    # Parse postgres section
    pg_section = spark_raw.get("postgres", {}) or postgres_raw.get("postgres", {}).get("connection", {})
    config.postgres.host = pg_section.get("host", config.postgres.host)
    config.postgres.port = int(pg_section.get("port", config.postgres.port))
    config.postgres.database = pg_section.get("database", config.postgres.database)
    config.postgres.username = pg_section.get("username", config.postgres.username)
    config.postgres.password = pg_section.get("password", config.postgres.password)

    pool = pg_section.get("pool", {}) or postgres_raw.get("postgres", {}).get("pool", {})
    config.postgres.pool_min = pool.get("min_size", config.postgres.pool_min)
    config.postgres.pool_max = pool.get("max_size", config.postgres.pool_max)

    batch = spark_raw.get("postgres", {}).get("batch", {}) or postgres_raw.get("batch", {})
    config.postgres.batch_size = batch.get("size", config.postgres.batch_size)
    config.postgres.batch_timeout = batch.get("interval_seconds", config.postgres.batch_timeout)

    # Parse REST API sink section
    api_raw = _resolve_dict(_load_yaml("api.yaml"))
    rest_section = spark_raw.get("rest_api", {})
    config.rest_api.enabled = rest_section.get("enabled", config.rest_api.enabled)
    api_server = api_raw.get("api", {}).get("server", {})
    api_host = rest_section.get("host", api_server.get("host", "localhost"))
    api_port = rest_section.get("port", api_server.get("port", 8000))
    config.rest_api.base_url = rest_section.get("base_url", f"http://{api_host}:{api_port}")
    config.rest_api.timeout = rest_section.get("timeout", config.rest_api.timeout)
    config.rest_api.consumer_id = rest_section.get("consumer_id", config.rest_api.consumer_id)

    # Parse transformation section
    transform = spark_raw.get("transformation", {})

    join_section = transform.get("join", {})
    config.join.window_duration = join_section.get("window_duration", config.join.window_duration)
    config.join.state_ttl = join_section.get("state_ttl", config.join.state_ttl)

    xml_section = transform.get("xml", {})
    config.xml.enabled = xml_section.get("enabled", config.xml.enabled)
    config.xml.column = xml_section.get("column", config.xml.column)
    config.xml.target_table = xml_section.get("target_table", config.xml.target_table)

    # Parse error handling
    error_section = spark_raw.get("error_handling", {})

    retry = error_section.get("retry", {})
    config.error_handling.retry.max_attempts = retry.get("max_attempts", config.error_handling.retry.max_attempts)
    config.error_handling.retry.initial_delay_ms = retry.get("initial_delay_ms", config.error_handling.retry.initial_delay_ms)
    config.error_handling.retry.max_delay_ms = retry.get("max_delay_ms", config.error_handling.retry.max_delay_ms)
    config.error_handling.retry.multiplier = retry.get("multiplier", config.error_handling.retry.multiplier)

    cb = error_section.get("circuit_breaker", {})
    config.error_handling.circuit_breaker.failure_threshold = cb.get("failure_threshold", config.error_handling.circuit_breaker.failure_threshold)
    config.error_handling.circuit_breaker.recovery_timeout = cb.get("recovery_timeout_seconds", config.error_handling.circuit_breaker.recovery_timeout)

    dlq = error_section.get("dlq", {})
    config.error_handling.dlq.enabled = dlq.get("enabled", config.error_handling.dlq.enabled)
    config.error_handling.dlq.topic_prefix = dlq.get("topic_prefix", config.error_handling.dlq.topic_prefix)

    # Logging
    config.log_level = spark_raw.get("logging", {}).get("level", config.log_level)

    return config


# Singleton
_config: Optional[SparkConsumerConfig] = None


def get_config() -> SparkConsumerConfig:
    """Get or create configuration singleton"""
    global _config
    if _config is None:
        _config = load_config()
    return _config
