"""
Order Enrichment - 3-Way Stream Join for PyFlink
Joins ORDERS + CUSTOMERS + ORDER_ITEMS using keyed state stores with TTL
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from ...spark_consumer.state_store import DimensionStateStore, OrderItemsStateStore

logger = logging.getLogger(__name__)


class OrderEnrichmentProcessor:
    """
    Stateful 3-way join processor with keyed state stores:
    - CUSTOMERS: DimensionStateStore keyed by customer_id (O(1) lookup, auto-dedup, TTL)
    - PRODUCTS: DimensionStateStore keyed by product_id
    - ORDER_ITEMS: OrderItemsStateStore grouped by order_id
    """

    def __init__(self, state_ttl_ms: int = 3600000):
        ttl_seconds = state_ttl_ms // 1000

        # Keyed state stores with TTL
        self._customers = DimensionStateStore("flink_customers", ttl_seconds=ttl_seconds)
        self._products = DimensionStateStore("flink_products", ttl_seconds=ttl_seconds)
        self._order_items = OrderItemsStateStore(ttl_seconds=ttl_seconds)

    def update_customer(self, record: Dict[str, Any]) -> None:
        """Update customer state (O(1) upsert, auto-dedup)"""
        customer_id = record.get("CUSTOMER_ID")
        if customer_id is not None:
            self._customers.put(customer_id, record)
            logger.debug(f"Customer state updated: {customer_id}")

    def update_product(self, record: Dict[str, Any]) -> None:
        """Update product state (O(1) upsert, auto-dedup)"""
        product_id = record.get("PRODUCT_ID")
        if product_id is not None:
            self._products.put(product_id, record)

    def add_order_item(self, record: Dict[str, Any]) -> None:
        """Add order item to state (grouped by order_id)"""
        order_id = record.get("ORDER_ID")
        if order_id is not None:
            self._order_items.add_item(order_id, record)

    def enrich_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single order with customer and item data.
        Returns enriched record matching cdc_orders_enriched table.
        """
        customer_id = order.get("CUSTOMER_ID")
        order_id = order.get("ORDER_ID")
        now = datetime.now()

        # Lookup customer (O(1) from keyed state, always latest)
        customer = self._customers.get(customer_id) or {}
        first_name = customer.get("FIRST_NAME", "")
        last_name = customer.get("LAST_NAME", "")
        customer_name = f"{first_name} {last_name}".strip() or None

        # Lookup items for this order
        item_count = self._order_items.get_item_count(order_id)

        enriched = {
            "order_id": order_id,
            "customer_id": customer_id,
            "customer_name": customer_name,
            "customer_tier": customer.get("TIER"),
            "customer_region": customer.get("CITY"),
            "total_amount": order.get("TOTAL_AMOUNT"),
            "item_count": item_count,
            "order_date": order.get("ORDER_DATE"),
            "status": order.get("STATUS"),
            "window_start": now,
            "window_end": now,
            "processed_at": now,
        }

        return enriched

    def enrich_batch(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich a batch of orders"""
        return [self.enrich_order(o) for o in orders]

    def aggregate_by_tier(
        self, enriched_orders: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Aggregate enriched orders by customer tier.
        Output matches cdc_order_aggregations table.
        """
        if not enriched_orders:
            return []

        by_tier: Dict[str, List[Dict[str, Any]]] = {}
        for o in enriched_orders:
            tier = o.get("customer_tier") or "UNKNOWN"
            by_tier.setdefault(tier, []).append(o)

        now = datetime.now()
        aggregations = []
        for tier, orders in by_tier.items():
            amounts = [o.get("total_amount", 0) or 0 for o in orders]
            aggregations.append({
                "window_start": now,
                "window_end": now,
                "customer_tier": tier,
                "product_category": None,
                "order_count": len(orders),
                "total_revenue": round(sum(amounts), 2),
                "avg_order_value": round(sum(amounts) / len(amounts), 2) if amounts else 0,
                "processed_at": now,
            })

        return aggregations

    def get_state_stats(self) -> Dict[str, int]:
        """Get state store sizes"""
        return {
            "customers": self._customers.size(),
            "products": self._products.size(),
            "order_items_keys": self._order_items.size(),
            "order_items_total": self._order_items.total_items(),
        }

    def get_customer_store(self) -> DimensionStateStore:
        return self._customers

    def get_product_store(self) -> DimensionStateStore:
        return self._products

    def get_order_items_store(self) -> OrderItemsStateStore:
        return self._order_items
