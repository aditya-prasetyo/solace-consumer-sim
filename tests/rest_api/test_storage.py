"""
Tests for REST API In-Memory Storage
Covers: InMemoryStore (add, get, TTL, eviction), EventStoreManager (metrics)
"""

import time
import pytest

from src.rest_api.storage import InMemoryStore, EventStoreManager


class TestInMemoryStore:

    def test_add_and_get(self):
        store = InMemoryStore(max_size=100, ttl_seconds=3600)
        eid = store.add({"table": "ORDERS", "data": {"ORDER_ID": 1}})

        result = store.get(eid)
        assert result is not None
        assert result["table"] == "ORDERS"
        assert result["_id"] == eid

    def test_add_custom_id(self):
        store = InMemoryStore()
        eid = store.add({"data": "test"}, event_id="custom-123")
        assert eid == "custom-123"
        assert store.get("custom-123") is not None

    def test_get_missing(self):
        store = InMemoryStore()
        assert store.get("nonexistent") is None

    def test_ttl_expiry(self):
        store = InMemoryStore(ttl_seconds=1)
        eid = store.add({"data": "test"})

        assert store.get(eid) is not None
        time.sleep(1.1)
        assert store.get(eid) is None

    def test_max_size_eviction(self):
        store = InMemoryStore(max_size=3)
        ids = [store.add({"n": i}) for i in range(5)]

        assert store.count() == 3
        # Oldest entries evicted
        assert store.get(ids[0]) is None
        assert store.get(ids[1]) is None
        assert store.get(ids[4]) is not None

    def test_get_all_with_pagination(self):
        store = InMemoryStore()
        for i in range(10):
            store.add({"n": i})

        all_items = store.get_all(limit=5)
        assert len(all_items) == 5

        page2 = store.get_all(limit=5, offset=5)
        assert len(page2) == 5

    def test_get_by_filter(self):
        store = InMemoryStore()
        store.add({"table": "ORDERS", "n": 1})
        store.add({"table": "CUSTOMERS", "n": 2})
        store.add({"table": "ORDERS", "n": 3})

        results = store.get_by_filter("table", "ORDERS")
        assert len(results) == 2

    def test_count(self):
        store = InMemoryStore()
        store.add({"a": 1})
        store.add({"b": 2})
        assert store.count() == 2

    def test_clear(self):
        store = InMemoryStore()
        store.add({"a": 1})
        store.clear()
        assert store.count() == 0


class TestEventStoreManager:

    def test_record_event(self):
        manager = EventStoreManager()
        manager.record_event("ORDERS", "c")
        manager.record_event("ORDERS", "u")
        manager.record_event("CUSTOMERS", "c")

        metrics = manager.get_metrics()
        assert metrics["total_events"] == 3
        assert metrics["events_by_table"]["ORDERS"] == 2
        assert metrics["events_by_operation"]["c"] == 2

    def test_record_error(self):
        manager = EventStoreManager()
        manager.record_error()
        manager.record_error()

        metrics = manager.get_metrics()
        assert metrics["errors"] == 2

    def test_metrics_uptime(self):
        manager = EventStoreManager()
        time.sleep(0.1)

        metrics = manager.get_metrics()
        assert metrics["uptime_seconds"] >= 0.1

    def test_stores_exist(self):
        manager = EventStoreManager()
        assert manager.events is not None
        assert manager.enriched_orders is not None
        assert manager.extracted_items is not None
        assert manager.notifications is not None
