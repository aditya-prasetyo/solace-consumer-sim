"""
Tests for PyFlink CDC Event Parser and Deduplicator
Covers: parse operations (c/r/u/d), raw JSON parsing, dedup by key, batch dedup
"""

import pytest

from src.flink_consumer.processors.cdc_processor import (
    CDCEventParser,
    Deduplicator,
    TABLE_KEY_MAP,
)


class TestCDCEventParser:

    def setup_method(self):
        self.parser = CDCEventParser()

    def test_parse_insert(self):
        payload = {
            "op": "c",
            "ts_ms": 1700000000000,
            "source": {"table": "ORDERS", "scn": "123"},
            "after": {"ORDER_ID": 1001, "TOTAL_AMOUNT": 299.99},
        }
        result = self.parser.parse(payload, "ORDERS")

        assert result is not None
        assert result["ORDER_ID"] == 1001
        assert result["_cdc_op"] == "c"
        assert result["_cdc_ts_ms"] == 1700000000000
        assert result["_cdc_table"] == "ORDERS"

    def test_parse_snapshot(self):
        payload = {
            "op": "r",
            "ts_ms": 1700000000000,
            "source": {"table": "CUSTOMERS"},
            "after": {"CUSTOMER_ID": 501},
        }
        result = self.parser.parse(payload, "CUSTOMERS")
        assert result is not None
        assert result["_cdc_op"] == "r"

    def test_parse_update(self):
        payload = {
            "op": "u",
            "ts_ms": 1700000000000,
            "source": {},
            "after": {"ORDER_ID": 1001, "STATUS": "SHIPPED"},
            "before": {"ORDER_ID": 1001, "STATUS": "PENDING"},
        }
        result = self.parser.parse(payload, "ORDERS")
        assert result["STATUS"] == "SHIPPED"
        assert result["_cdc_op"] == "u"

    def test_parse_delete(self):
        payload = {
            "op": "d",
            "ts_ms": 1700000000000,
            "source": {},
            "before": {"ORDER_ID": 1001, "STATUS": "CANCELLED"},
        }
        result = self.parser.parse(payload, "ORDERS")
        assert result["ORDER_ID"] == 1001
        assert result["_cdc_op"] == "d"

    def test_parse_unknown_op_returns_none(self):
        payload = {"op": "x", "ts_ms": 0, "source": {}, "after": {"ID": 1}}
        result = self.parser.parse(payload, "ORDERS")
        assert result is None

    def test_parse_empty_data_returns_none(self):
        payload = {"op": "c", "ts_ms": 0, "source": {}, "after": {}}
        result = self.parser.parse(payload, "ORDERS")
        assert result is None

    def test_parse_source_ts_generated(self):
        payload = {
            "op": "c",
            "ts_ms": 1700000000000,
            "source": {},
            "after": {"ORDER_ID": 1},
        }
        result = self.parser.parse(payload, "ORDERS")
        assert "_source_ts" in result

    def test_parse_raw_json(self):
        json_str = '{"payload": {"op": "c", "ts_ms": 1700000000000, "source": {"table": "ORDERS"}, "after": {"ORDER_ID": 1001}}}'
        result = self.parser.parse_raw_json(json_str, "ORDERS")
        assert result is not None
        assert result["ORDER_ID"] == 1001

    def test_parse_raw_json_invalid(self):
        result = self.parser.parse_raw_json("not json at all", "ORDERS")
        assert result is None

    def test_parse_raw_json_flat_payload(self):
        """JSON without wrapper payload key"""
        json_str = '{"op": "c", "ts_ms": 1700000000000, "source": {}, "after": {"CUSTOMER_ID": 1}}'
        result = self.parser.parse_raw_json(json_str, "CUSTOMERS")
        assert result is not None
        assert result["CUSTOMER_ID"] == 1


class TestDeduplicator:

    def setup_method(self):
        self.dedup = Deduplicator(max_entries=100)

    def test_first_record_not_duplicate(self):
        record = {"ORDER_ID": 1001, "_cdc_ts_ms": 1000}
        assert self.dedup.is_duplicate("ORDERS", record) is False

    def test_same_key_older_ts_is_duplicate(self):
        self.dedup.is_duplicate("ORDERS", {"ORDER_ID": 1001, "_cdc_ts_ms": 2000})
        assert self.dedup.is_duplicate("ORDERS", {"ORDER_ID": 1001, "_cdc_ts_ms": 1000}) is True

    def test_same_key_newer_ts_not_duplicate(self):
        self.dedup.is_duplicate("ORDERS", {"ORDER_ID": 1001, "_cdc_ts_ms": 1000})
        assert self.dedup.is_duplicate("ORDERS", {"ORDER_ID": 1001, "_cdc_ts_ms": 2000}) is False

    def test_different_keys_not_duplicate(self):
        self.dedup.is_duplicate("ORDERS", {"ORDER_ID": 1001, "_cdc_ts_ms": 1000})
        assert self.dedup.is_duplicate("ORDERS", {"ORDER_ID": 1002, "_cdc_ts_ms": 1000}) is False

    def test_no_key_column_not_duplicate(self):
        record = {"UNKNOWN_ID": 1, "_cdc_ts_ms": 1000}
        assert self.dedup.is_duplicate("ORDERS", record) is False

    def test_deduplicate_batch_keeps_latest(self):
        records = [
            {"ORDER_ID": 1001, "_cdc_ts_ms": 1000, "STATUS": "PENDING"},
            {"ORDER_ID": 1001, "_cdc_ts_ms": 3000, "STATUS": "SHIPPED"},
            {"ORDER_ID": 1001, "_cdc_ts_ms": 2000, "STATUS": "PROCESSING"},
            {"ORDER_ID": 1002, "_cdc_ts_ms": 1500, "STATUS": "PENDING"},
        ]
        result = self.dedup.deduplicate_batch("ORDERS", records)

        assert len(result) == 2
        result_by_id = {r["ORDER_ID"]: r for r in result}
        assert result_by_id[1001]["STATUS"] == "SHIPPED"
        assert result_by_id[1002]["STATUS"] == "PENDING"

    def test_deduplicate_batch_unknown_table(self):
        records = [{"ID": 1}, {"ID": 2}]
        result = self.dedup.deduplicate_batch("UNKNOWN", records)
        assert result == records

    def test_eviction_on_overflow(self):
        """Entries evicted when exceeding max_entries"""
        dedup = Deduplicator(max_entries=10)
        for i in range(20):
            dedup.is_duplicate("ORDERS", {"ORDER_ID": i, "_cdc_ts_ms": i * 1000})

        # Should not grow unbounded
        assert len(dedup._seen) <= 15  # Some eviction happened


class TestTableKeyMap:

    def test_known_tables(self):
        assert TABLE_KEY_MAP["ORDERS"] == "ORDER_ID"
        assert TABLE_KEY_MAP["ORDER_ITEMS"] == "ITEM_ID"
        assert TABLE_KEY_MAP["CUSTOMERS"] == "CUSTOMER_ID"
        assert TABLE_KEY_MAP["PRODUCTS"] == "PRODUCT_ID"
        assert TABLE_KEY_MAP["AUDIT_LOG"] == "LOG_ID"
