"""
Tests for PyFlink Order Enrichment Processor
Covers: state updates, 3-way join enrichment, aggregation by tier, state stats
"""

import pytest

from src.spark_consumer.state_store import DimensionStateStore, OrderItemsStateStore
from src.flink_consumer.processors.order_enrichment import OrderEnrichmentProcessor


class TestOrderEnrichmentStateUpdates:

    def setup_method(self):
        self.processor = OrderEnrichmentProcessor(state_ttl_ms=3600000)

    def test_update_customer(self, sample_customer_data):
        self.processor.update_customer(sample_customer_data)

        store = self.processor.get_customer_store()
        assert store.size() == 1
        assert store.get(501)["FIRST_NAME"] == "John"

    def test_update_customer_upsert(self, sample_customer_data):
        """Same customer_id updates, no duplicates"""
        self.processor.update_customer(sample_customer_data)
        updated = {**sample_customer_data, "TIER": "PLATINUM"}
        self.processor.update_customer(updated)

        store = self.processor.get_customer_store()
        assert store.size() == 1
        assert store.get(501)["TIER"] == "PLATINUM"

    def test_update_customer_no_id(self):
        self.processor.update_customer({"NAME": "No ID"})
        assert self.processor.get_customer_store().size() == 0

    def test_update_product(self, sample_product_data):
        self.processor.update_product(sample_product_data)

        store = self.processor.get_product_store()
        assert store.size() == 1
        assert store.get(10)["NAME"] == "Widget Pro"

    def test_add_order_item(self, sample_order_item_data):
        self.processor.add_order_item(sample_order_item_data)

        store = self.processor.get_order_items_store()
        assert store.get_item_count(1001) == 1

    def test_multiple_items_same_order(self):
        self.processor.add_order_item({"ORDER_ID": 1001, "ITEM_ID": 1})
        self.processor.add_order_item({"ORDER_ID": 1001, "ITEM_ID": 2})
        self.processor.add_order_item({"ORDER_ID": 1001, "ITEM_ID": 3})

        assert self.processor.get_order_items_store().get_item_count(1001) == 3


class TestOrderEnrichment:

    def setup_method(self):
        self.processor = OrderEnrichmentProcessor(state_ttl_ms=3600000)

    def test_enrich_with_customer(self, sample_customer_data):
        """Enrichment joins customer name and tier"""
        self.processor.update_customer(sample_customer_data)

        order = {
            "ORDER_ID": 1001,
            "CUSTOMER_ID": 501,
            "TOTAL_AMOUNT": 299.99,
            "ORDER_DATE": "2024-01-15",
            "STATUS": "PENDING",
        }

        enriched = self.processor.enrich_order(order)
        assert enriched["order_id"] == 1001
        assert enriched["customer_name"] == "John Doe"
        assert enriched["customer_tier"] == "GOLD"
        assert enriched["customer_region"] == "Jakarta"
        assert enriched["total_amount"] == 299.99

    def test_enrich_without_customer(self):
        """Enrichment works even without customer state"""
        order = {
            "ORDER_ID": 1001,
            "CUSTOMER_ID": 999,
            "TOTAL_AMOUNT": 100.0,
            "STATUS": "PENDING",
        }

        enriched = self.processor.enrich_order(order)
        assert enriched["order_id"] == 1001
        assert enriched["customer_name"] is None
        assert enriched["customer_tier"] is None

    def test_enrich_with_items(self):
        """Enrichment includes item count"""
        self.processor.add_order_item({"ORDER_ID": 1001, "ITEM_ID": 1})
        self.processor.add_order_item({"ORDER_ID": 1001, "ITEM_ID": 2})

        order = {"ORDER_ID": 1001, "CUSTOMER_ID": 501, "TOTAL_AMOUNT": 200.0, "STATUS": "PENDING"}
        enriched = self.processor.enrich_order(order)

        assert enriched["item_count"] == 2

    def test_enrich_batch(self, sample_customer_data):
        self.processor.update_customer(sample_customer_data)

        orders = [
            {"ORDER_ID": 1001, "CUSTOMER_ID": 501, "TOTAL_AMOUNT": 100.0, "STATUS": "PENDING"},
            {"ORDER_ID": 1002, "CUSTOMER_ID": 501, "TOTAL_AMOUNT": 200.0, "STATUS": "SHIPPED"},
        ]

        results = self.processor.enrich_batch(orders)
        assert len(results) == 2
        assert results[0]["customer_name"] == "John Doe"
        assert results[1]["customer_name"] == "John Doe"

    def test_enriched_output_fields(self, sample_customer_data):
        """Verify enriched output has all required fields"""
        self.processor.update_customer(sample_customer_data)

        order = {"ORDER_ID": 1001, "CUSTOMER_ID": 501, "TOTAL_AMOUNT": 299.99, "STATUS": "PENDING"}
        enriched = self.processor.enrich_order(order)

        expected_keys = {
            "order_id", "customer_id", "customer_name", "customer_tier",
            "customer_region", "total_amount", "item_count", "order_date",
            "status", "window_start", "window_end", "processed_at",
        }
        assert set(enriched.keys()) == expected_keys


class TestOrderAggregation:

    def setup_method(self):
        self.processor = OrderEnrichmentProcessor(state_ttl_ms=3600000)

    def test_aggregate_by_tier(self):
        enriched = [
            {"customer_tier": "GOLD", "total_amount": 100.0},
            {"customer_tier": "GOLD", "total_amount": 200.0},
            {"customer_tier": "SILVER", "total_amount": 50.0},
        ]

        aggs = self.processor.aggregate_by_tier(enriched)
        assert len(aggs) == 2

        by_tier = {a["customer_tier"]: a for a in aggs}
        assert by_tier["GOLD"]["order_count"] == 2
        assert by_tier["GOLD"]["total_revenue"] == 300.0
        assert by_tier["GOLD"]["avg_order_value"] == 150.0
        assert by_tier["SILVER"]["order_count"] == 1
        assert by_tier["SILVER"]["total_revenue"] == 50.0

    def test_aggregate_empty(self):
        assert self.processor.aggregate_by_tier([]) == []

    def test_aggregate_unknown_tier(self):
        enriched = [
            {"customer_tier": None, "total_amount": 100.0},
        ]
        aggs = self.processor.aggregate_by_tier(enriched)
        assert len(aggs) == 1
        assert aggs[0]["customer_tier"] == "UNKNOWN"

    def test_aggregate_output_fields(self):
        enriched = [
            {"customer_tier": "GOLD", "total_amount": 100.0},
        ]
        aggs = self.processor.aggregate_by_tier(enriched)
        expected_keys = {
            "window_start", "window_end", "customer_tier", "product_category",
            "order_count", "total_revenue", "avg_order_value", "processed_at",
        }
        assert set(aggs[0].keys()) == expected_keys


class TestStateStats:

    def test_get_state_stats(self):
        processor = OrderEnrichmentProcessor(state_ttl_ms=3600000)
        processor.update_customer({"CUSTOMER_ID": 1})
        processor.update_customer({"CUSTOMER_ID": 2})
        processor.update_product({"PRODUCT_ID": 10})
        processor.add_order_item({"ORDER_ID": 1001, "ITEM_ID": 1})
        processor.add_order_item({"ORDER_ID": 1001, "ITEM_ID": 2})

        stats = processor.get_state_stats()
        assert stats["customers"] == 2
        assert stats["products"] == 1
        assert stats["order_items_keys"] == 1
        assert stats["order_items_total"] == 2

    def test_store_accessors(self):
        processor = OrderEnrichmentProcessor(state_ttl_ms=3600000)
        assert isinstance(processor.get_customer_store(), DimensionStateStore)
        assert isinstance(processor.get_product_store(), DimensionStateStore)
        assert isinstance(processor.get_order_items_store(), OrderItemsStateStore)
