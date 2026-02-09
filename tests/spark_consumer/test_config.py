"""
Tests for PySpark Consumer Config
Covers: defaults, env resolution, config structure
"""

import os
import pytest

from src.spark_consumer.config import (
    SparkConsumerConfig,
    SparkConfig,
    SolaceSourceConfig,
    PostgresConfig,
    JoinConfig,
    ErrorHandlingConfig,
    _resolve_env,
)


class TestResolveEnv:

    def test_plain_text(self):
        assert _resolve_env("hello") == "hello"

    def test_env_with_default(self):
        result = _resolve_env("${NONEXISTENT:fallback_val}")
        assert result == "fallback_val"

    def test_env_set(self):
        os.environ["TEST_SPARK_VAR"] = "my_value"
        try:
            assert _resolve_env("${TEST_SPARK_VAR:def}") == "my_value"
        finally:
            del os.environ["TEST_SPARK_VAR"]

    def test_non_string_passthrough(self):
        assert _resolve_env(123) == 123


class TestSparkConfigDefaults:

    def test_spark_defaults(self):
        config = SparkConfig()
        assert config.app_name == "CDC-Spark-Consumer"
        assert config.master == "local[*]"
        assert config.shuffle_partitions == 4

    def test_solace_defaults(self):
        config = SolaceSourceConfig()
        assert config.host == "localhost"
        assert config.port == 55555
        assert len(config.topics) == 5
        assert "cdc/oracle/SALES/ORDERS/*" in config.topics

    def test_postgres_defaults(self):
        config = PostgresConfig()
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "ods"
        assert config.batch_size == 1000

    def test_postgres_urls(self):
        config = PostgresConfig(
            username="user", password="pass", host="db", port=5432, database="ods"
        )
        assert config.jdbc_url == "jdbc:postgresql://db:5432/ods"
        assert config.connection_url == "postgresql://user:pass@db:5432/ods"

    def test_join_config_defaults(self):
        config = JoinConfig()
        assert config.window_duration == "5 minutes"
        assert config.state_ttl == "1 hour"

    def test_error_handling_defaults(self):
        config = ErrorHandlingConfig()
        assert config.retry.max_attempts == 3
        assert config.circuit_breaker.failure_threshold == 5
        assert config.dlq.enabled is True

    def test_full_config(self):
        config = SparkConsumerConfig()
        assert config.spark.app_name == "CDC-Spark-Consumer"
        assert config.streaming.trigger_interval == "10 seconds"
        assert config.rest_api.enabled is True
        assert config.log_level == "INFO"
