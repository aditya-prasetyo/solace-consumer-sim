"""
Tests for shared DimensionStateStore and OrderItemsStateStore
Covers: TTL, O(1) keyed upsert, auto-dedup, expiry, size tracking
"""

import time
import pytest

from src.spark_consumer.state_store import (
    DimensionStateStore,
    OrderItemsStateStore,
    StateEntry,
)


class TestDimensionStateStore:
    """Tests for keyed dimension state store"""

    def test_put_and_get(self, sample_customer_data):
        store = DimensionStateStore("test_customers", ttl_seconds=3600)
        store.put(501, sample_customer_data)

        result = store.get(501)
        assert result is not None
        assert result["CUSTOMER_ID"] == 501
        assert result["FIRST_NAME"] == "John"
        assert result["TIER"] == "GOLD"

    def test_get_missing_key_returns_none(self):
        store = DimensionStateStore("test", ttl_seconds=3600)
        assert store.get(999) is None

    def test_upsert_overwrites_previous(self, sample_customer_data):
        """Keyed upsert: latest value per key, auto-dedup"""
        store = DimensionStateStore("test", ttl_seconds=3600)

        store.put(501, sample_customer_data)
        assert store.get(501)["TIER"] == "GOLD"

        # Update same key with new tier
        updated = {**sample_customer_data, "TIER": "PLATINUM"}
        store.put(501, updated)

        result = store.get(501)
        assert result["TIER"] == "PLATINUM"
        assert store.size() == 1  # No duplicates

    def test_multiple_keys(self):
        store = DimensionStateStore("test", ttl_seconds=3600)

        store.put(1, {"CUSTOMER_ID": 1, "NAME": "A"})
        store.put(2, {"CUSTOMER_ID": 2, "NAME": "B"})
        store.put(3, {"CUSTOMER_ID": 3, "NAME": "C"})

        assert store.size() == 3
        assert store.get(2)["NAME"] == "B"

    def test_ttl_expiry(self):
        """Records expire after TTL"""
        store = DimensionStateStore("test", ttl_seconds=1)
        store.put(1, {"ID": 1})

        assert store.get(1) is not None

        time.sleep(1.1)
        assert store.get(1) is None  # Expired

    def test_expire_removes_old_entries(self):
        store = DimensionStateStore("test", ttl_seconds=1)
        store.put(1, {"ID": 1})
        store.put(2, {"ID": 2})

        time.sleep(1.1)
        removed = store.expire()

        assert removed == 2
        assert store.size() == 0

    def test_get_all_excludes_expired(self):
        store = DimensionStateStore("test", ttl_seconds=1)
        store.put(1, {"ID": 1})
        time.sleep(1.1)
        store.put(2, {"ID": 2})  # Fresh entry

        all_data = store.get_all()
        assert len(all_data) == 1
        assert 2 in all_data

    def test_get_all_values(self):
        store = DimensionStateStore("test", ttl_seconds=3600)
        store.put(1, {"ID": 1})
        store.put(2, {"ID": 2})

        values = store.get_all_values()
        assert len(values) == 2
        ids = {v["ID"] for v in values}
        assert ids == {1, 2}

    def test_remove(self):
        store = DimensionStateStore("test", ttl_seconds=3600)
        store.put(1, {"ID": 1})
        store.remove(1)
        assert store.get(1) is None
        assert store.size() == 0

    def test_clear(self):
        store = DimensionStateStore("test", ttl_seconds=3600)
        for i in range(10):
            store.put(i, {"ID": i})
        assert store.size() == 10

        store.clear()
        assert store.size() == 0

    def test_periodic_expiry_on_put(self):
        """Every 500 puts should trigger expiry check"""
        store = DimensionStateStore("test", ttl_seconds=1)

        # Add and let expire
        store.put(0, {"ID": 0})
        time.sleep(1.1)

        # Trigger periodic expiry by doing 500 more puts
        for i in range(1, 501):
            store.put(i, {"ID": i})

        # Entry 0 should have been expired during periodic check
        assert store.get(0) is None

    def test_1000_updates_100_customers_memory_efficiency(self):
        """1000 CDC updates for 100 customers = 100 entries (not 1000)"""
        store = DimensionStateStore("test", ttl_seconds=3600)

        for batch in range(10):
            for cid in range(100):
                store.put(cid, {
                    "CUSTOMER_ID": cid,
                    "TIER": f"TIER_{batch}",
                    "BATCH": batch,
                })

        assert store.size() == 100  # Not 1000
        # Each customer has latest batch value
        assert store.get(0)["BATCH"] == 9
        assert store.get(99)["BATCH"] == 9


class TestOrderItemsStateStore:
    """Tests for order items state store"""

    def test_add_and_get_items(self, sample_order_item_data):
        store = OrderItemsStateStore(ttl_seconds=3600)
        store.add_item(1001, sample_order_item_data)

        items = store.get_items(1001)
        assert len(items) == 1
        assert items[0]["PRODUCT_ID"] == 10

    def test_multiple_items_per_order(self):
        store = OrderItemsStateStore(ttl_seconds=3600)

        store.add_item(1001, {"ITEM_ID": 1, "PRODUCT_ID": 10})
        store.add_item(1001, {"ITEM_ID": 2, "PRODUCT_ID": 20})
        store.add_item(1001, {"ITEM_ID": 3, "PRODUCT_ID": 30})

        assert store.get_item_count(1001) == 3
        assert store.size() == 1  # 1 unique order_id

    def test_items_grouped_by_order(self):
        store = OrderItemsStateStore(ttl_seconds=3600)

        store.add_item(1001, {"ITEM_ID": 1})
        store.add_item(1002, {"ITEM_ID": 2})
        store.add_item(1001, {"ITEM_ID": 3})

        assert store.get_item_count(1001) == 2
        assert store.get_item_count(1002) == 1
        assert store.size() == 2
        assert store.total_items() == 3

    def test_get_items_missing_order(self):
        store = OrderItemsStateStore(ttl_seconds=3600)
        assert store.get_items(9999) == []
        assert store.get_item_count(9999) == 0

    def test_ttl_expiry(self):
        store = OrderItemsStateStore(ttl_seconds=1)
        store.add_item(1001, {"ITEM_ID": 1})

        assert store.get_item_count(1001) == 1

        time.sleep(1.1)
        assert store.get_items(1001) == []

    def test_items_bound_per_order(self):
        """Items per order are bounded to prevent memory leaks"""
        store = OrderItemsStateStore(ttl_seconds=3600)

        for i in range(150):
            store.add_item(1001, {"ITEM_ID": i})

        # Should be bounded (kept last 50 after hitting 100)
        items = store.get_items(1001)
        assert len(items) <= 100

    def test_get_all_items_flat(self):
        store = OrderItemsStateStore(ttl_seconds=3600)

        store.add_item(1001, {"ITEM_ID": 1})
        store.add_item(1001, {"ITEM_ID": 2})
        store.add_item(1002, {"ITEM_ID": 3})

        flat = store.get_all_items_flat()
        assert len(flat) == 3

    def test_clear(self):
        store = OrderItemsStateStore(ttl_seconds=3600)
        store.add_item(1001, {"ITEM_ID": 1})
        store.add_item(1002, {"ITEM_ID": 2})

        store.clear()
        assert store.size() == 0
        assert store.total_items() == 0

    def test_expired_order_gets_fresh_items(self):
        """After TTL expires, new add_item starts fresh list"""
        store = OrderItemsStateStore(ttl_seconds=1)
        store.add_item(1001, {"ITEM_ID": 1, "OLD": True})

        time.sleep(1.1)

        store.add_item(1001, {"ITEM_ID": 2, "OLD": False})
        items = store.get_items(1001)
        assert len(items) == 1
        assert items[0]["OLD"] is False
