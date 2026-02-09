"""
PyFlink Consumer Configuration
Loads and manages all configuration for Architecture B
"""

import os
import re
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "configs")


def _resolve_env(value: str) -> str:
    """Resolve ${ENV_VAR:default} patterns"""
    if not isinstance(value, str) or "${" not in value:
        return value
    pattern = r"\$\{([^:}]+)(?::([^}]*))?\}"
    def replacer(match):
        return os.environ.get(match.group(1), match.group(2) or "")
    return re.sub(pattern, replacer, value)


def _resolve_dict(d: dict) -> dict:
    """Recursively resolve environment variables"""
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
class FlinkConfig:
    """Flink application configuration"""
    app_name: str = "CDC-Flink-Consumer"
    parallelism: int = 2
    taskmanager_memory: str = "1600m"
    taskmanager_slots: int = 2
    web_port: int = 8081


@dataclass
class CheckpointConfig:
    """Flink checkpoint configuration"""
    enabled: bool = True
    interval_ms: int = 10000
    mode: str = "EXACTLY_ONCE"
    storage: str = "/checkpoints"
    min_pause_ms: int = 5000
    timeout_ms: int = 60000


@dataclass
class StateConfig:
    """State backend configuration"""
    backend: str = "hashmap"
    ttl_ms: int = 3600000


@dataclass
class WatermarkConfig:
    """Watermark configuration"""
    strategy: str = "bounded_out_of_orderness"
    max_out_of_orderness_ms: int = 10000


@dataclass
class RestartConfig:
    """Restart strategy"""
    strategy: str = "fixed_delay"
    attempts: int = 3
    delay_ms: int = 10000


@dataclass
class SolaceSourceConfig:
    """Solace connection configuration"""
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
    ])
    queue: str = "cdc-flink-consumer"
    reconnect_retries: int = -1
    reconnect_retry_wait_ms: int = 3000


@dataclass
class PostgresSinkConfig:
    """PostgreSQL ODS sink configuration"""
    enabled: bool = True
    host: str = "localhost"
    port: int = 5432
    database: str = "ods"
    username: str = "cdc_user"
    password: str = "cdc_pass"
    batch_size: int = 500
    batch_interval: int = 5

    @property
    def connection_url(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class RestApiSinkConfig:
    """REST API sink configuration"""
    enabled: bool = True
    url: str = "http://localhost:8000"
    events_endpoint: str = "/api/v1/events"
    notifications_endpoint: str = "/api/v1/notifications"
    timeout: int = 30
    max_connections: int = 10
    retry_attempts: int = 3
    batch_enabled: bool = True
    batch_size: int = 100
    batch_interval_ms: int = 1000
    consumer_id: str = "flink-consumer-1"


@dataclass
class JoinConfig:
    """Stream join configuration"""
    window_duration_ms: int = 300000


@dataclass
class XMLConfig:
    """XML extraction configuration"""
    enabled: bool = True
    column: str = "shipping_info"


@dataclass
class AlertConfig:
    """Alert rules"""
    high_value_threshold: float = 10000
    high_value_enabled: bool = True
    new_customer_tier: str = "GOLD"
    new_customer_enabled: bool = True


@dataclass
class RetryConfig:
    max_attempts: int = 3
    initial_delay_ms: int = 1000
    max_delay_ms: int = 30000
    multiplier: float = 2.0


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout: int = 30


@dataclass
class DLQConfig:
    enabled: bool = True
    topic_prefix: str = "cdc/dlq"


@dataclass
class ErrorHandlingConfig:
    retry: RetryConfig = field(default_factory=RetryConfig)
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    dlq: DLQConfig = field(default_factory=DLQConfig)


@dataclass
class FlinkConsumerConfig:
    """Root configuration for PyFlink Consumer"""
    flink: FlinkConfig = field(default_factory=FlinkConfig)
    checkpoint: CheckpointConfig = field(default_factory=CheckpointConfig)
    state: StateConfig = field(default_factory=StateConfig)
    watermark: WatermarkConfig = field(default_factory=WatermarkConfig)
    restart: RestartConfig = field(default_factory=RestartConfig)
    solace: SolaceSourceConfig = field(default_factory=SolaceSourceConfig)
    postgres: PostgresSinkConfig = field(default_factory=PostgresSinkConfig)
    rest_api: RestApiSinkConfig = field(default_factory=RestApiSinkConfig)
    join: JoinConfig = field(default_factory=JoinConfig)
    xml: XMLConfig = field(default_factory=XMLConfig)
    alerts: AlertConfig = field(default_factory=AlertConfig)
    error_handling: ErrorHandlingConfig = field(default_factory=ErrorHandlingConfig)
    log_level: str = "INFO"


def _load_yaml(filename: str) -> dict:
    filepath = os.path.join(CONFIG_DIR, filename)
    if not os.path.exists(filepath):
        return {}
    with open(filepath) as f:
        return yaml.safe_load(f) or {}


def load_config() -> FlinkConsumerConfig:
    """Load configuration from flink.yaml"""
    raw = _resolve_dict(_load_yaml("flink.yaml"))
    config = FlinkConsumerConfig()

    # Flink
    flink = raw.get("flink", {})
    config.flink.app_name = flink.get("app_name", config.flink.app_name)
    config.flink.parallelism = flink.get("parallelism", config.flink.parallelism)
    tm = flink.get("taskmanager", {})
    config.flink.taskmanager_memory = tm.get("memory", config.flink.taskmanager_memory)
    config.flink.taskmanager_slots = tm.get("slots", config.flink.taskmanager_slots)
    config.flink.web_port = flink.get("web", {}).get("port", config.flink.web_port)

    # Checkpoint
    cp = flink.get("checkpoint", {})
    config.checkpoint.enabled = cp.get("enabled", config.checkpoint.enabled)
    config.checkpoint.interval_ms = cp.get("interval_ms", config.checkpoint.interval_ms)
    config.checkpoint.mode = cp.get("mode", config.checkpoint.mode)
    config.checkpoint.storage = cp.get("storage", config.checkpoint.storage)
    config.checkpoint.min_pause_ms = cp.get("min_pause_ms", config.checkpoint.min_pause_ms)
    config.checkpoint.timeout_ms = cp.get("timeout_ms", config.checkpoint.timeout_ms)

    # State
    st = flink.get("state", {})
    config.state.backend = st.get("backend", config.state.backend)
    config.state.ttl_ms = st.get("ttl_ms", config.state.ttl_ms)

    # Watermark
    wm = flink.get("watermark", {})
    config.watermark.strategy = wm.get("strategy", config.watermark.strategy)
    config.watermark.max_out_of_orderness_ms = wm.get("max_out_of_orderness_ms", config.watermark.max_out_of_orderness_ms)

    # Restart
    rs = flink.get("restart", {})
    config.restart.strategy = rs.get("strategy", config.restart.strategy)
    config.restart.attempts = rs.get("attempts", config.restart.attempts)
    config.restart.delay_ms = rs.get("delay_ms", config.restart.delay_ms)

    # Solace
    sol = raw.get("solace", {})
    config.solace.host = sol.get("host", config.solace.host)
    config.solace.port = int(sol.get("port", config.solace.port))
    config.solace.vpn = sol.get("vpn", config.solace.vpn)
    config.solace.username = sol.get("username", config.solace.username)
    config.solace.password = sol.get("password", config.solace.password)
    config.solace.topics = sol.get("topics", config.solace.topics)
    config.solace.queue = sol.get("queue", config.solace.queue)

    # PostgreSQL
    pg = raw.get("postgres", {})
    config.postgres.enabled = pg.get("enabled", config.postgres.enabled)
    config.postgres.host = pg.get("host", config.postgres.host)
    config.postgres.port = int(pg.get("port", config.postgres.port))
    config.postgres.database = pg.get("database", config.postgres.database)
    config.postgres.username = pg.get("username", config.postgres.username)
    config.postgres.password = pg.get("password", config.postgres.password)
    pg_batch = pg.get("batch", {})
    config.postgres.batch_size = pg_batch.get("size", config.postgres.batch_size)
    config.postgres.batch_interval = pg_batch.get("interval_seconds", config.postgres.batch_interval)

    # REST API
    rest = raw.get("rest_api", {})
    config.rest_api.url = rest.get("url", config.rest_api.url)
    endpoints = rest.get("endpoints", {})
    config.rest_api.events_endpoint = endpoints.get("events", config.rest_api.events_endpoint)
    config.rest_api.notifications_endpoint = endpoints.get("notifications", config.rest_api.notifications_endpoint)
    client = rest.get("client", {})
    config.rest_api.timeout = client.get("timeout_seconds", config.rest_api.timeout)
    config.rest_api.max_connections = client.get("max_connections", config.rest_api.max_connections)
    config.rest_api.retry_attempts = client.get("retry_attempts", config.rest_api.retry_attempts)
    batch = rest.get("batch", {})
    config.rest_api.batch_enabled = batch.get("enabled", config.rest_api.batch_enabled)
    config.rest_api.batch_size = batch.get("size", config.rest_api.batch_size)
    config.rest_api.batch_interval_ms = batch.get("interval_ms", config.rest_api.batch_interval_ms)

    # Transformation
    transform = raw.get("transformation", {})
    config.join.window_duration_ms = transform.get("join", {}).get("window_duration_ms", config.join.window_duration_ms)
    xml = transform.get("xml", {})
    config.xml.enabled = xml.get("enabled", config.xml.enabled)
    config.xml.column = xml.get("column", config.xml.column)
    alerts = transform.get("alerts", {})
    hv = alerts.get("high_value_order", {})
    config.alerts.high_value_enabled = hv.get("enabled", config.alerts.high_value_enabled)
    config.alerts.high_value_threshold = hv.get("threshold", config.alerts.high_value_threshold)
    nc = alerts.get("new_customer", {})
    config.alerts.new_customer_enabled = nc.get("enabled", config.alerts.new_customer_enabled)
    config.alerts.new_customer_tier = nc.get("tier", config.alerts.new_customer_tier)

    # Error handling
    err = raw.get("error_handling", {})
    retry = err.get("retry", {})
    config.error_handling.retry.max_attempts = retry.get("max_attempts", config.error_handling.retry.max_attempts)
    config.error_handling.retry.initial_delay_ms = retry.get("initial_delay_ms", config.error_handling.retry.initial_delay_ms)
    config.error_handling.retry.max_delay_ms = retry.get("max_delay_ms", config.error_handling.retry.max_delay_ms)
    config.error_handling.retry.multiplier = retry.get("backoff_multiplier", config.error_handling.retry.multiplier)
    cb = err.get("circuit_breaker", {})
    config.error_handling.circuit_breaker.failure_threshold = cb.get("failure_threshold", config.error_handling.circuit_breaker.failure_threshold)
    config.error_handling.circuit_breaker.recovery_timeout = cb.get("recovery_timeout_seconds", config.error_handling.circuit_breaker.recovery_timeout)
    dlq = err.get("dlq", {})
    config.error_handling.dlq.enabled = dlq.get("enabled", config.error_handling.dlq.enabled)
    config.error_handling.dlq.topic_prefix = dlq.get("topic_prefix", config.error_handling.dlq.topic_prefix)

    # Logging
    config.log_level = raw.get("logging", {}).get("level", config.log_level)

    return config


_config: Optional[FlinkConsumerConfig] = None


def get_config() -> FlinkConsumerConfig:
    global _config
    if _config is None:
        _config = load_config()
    return _config
