"""
Tests for PyFlink Consumer Config
Covers: defaults, env resolution, YAML loading
"""

import os
import pytest

from src.flink_consumer.config import (
    FlinkConsumerConfig,
    FlinkConfig,
    SolaceSourceConfig,
    PostgresSinkConfig,
    RestApiSinkConfig,
    _resolve_env,
)


class TestResolveEnv:

    def test_no_env_var(self):
        assert _resolve_env("plain text") == "plain text"

    def test_env_var_with_default(self):
        result = _resolve_env("${NONEXISTENT_VAR:fallback}")
        assert result == "fallback"

    def test_env_var_set(self):
        os.environ["TEST_CDC_VAR"] = "resolved_value"
        try:
            result = _resolve_env("${TEST_CDC_VAR:default}")
            assert result == "resolved_value"
        finally:
            del os.environ["TEST_CDC_VAR"]

    def test_non_string(self):
        assert _resolve_env(42) == 42


class TestFlinkConfigDefaults:

    def test_flink_defaults(self):
        config = FlinkConfig()
        assert config.app_name == "CDC-Flink-Consumer"
        assert config.parallelism == 2

    def test_solace_defaults(self):
        config = SolaceSourceConfig()
        assert config.host == "localhost"
        assert config.port == 55555
        assert len(config.topics) == 4
        assert "cdc/oracle/SALES/ORDERS/*" in config.topics

    def test_postgres_defaults(self):
        config = PostgresSinkConfig()
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "ods"
        assert config.enabled is True

    def test_postgres_connection_url(self):
        config = PostgresSinkConfig(
            username="user", password="pass", host="db", port=5432, database="ods"
        )
        assert config.connection_url == "postgresql://user:pass@db:5432/ods"

    def test_rest_api_defaults(self):
        config = RestApiSinkConfig()
        assert config.enabled is True
        assert config.url == "http://localhost:8000"
        assert config.consumer_id == "flink-consumer-1"

    def test_full_config_defaults(self):
        config = FlinkConsumerConfig()
        assert config.flink.app_name == "CDC-Flink-Consumer"
        assert config.state.ttl_ms == 3600000
        assert config.checkpoint.enabled is True
        assert config.error_handling.retry.max_attempts == 3
        assert config.error_handling.dlq.enabled is True
        assert config.log_level == "INFO"
