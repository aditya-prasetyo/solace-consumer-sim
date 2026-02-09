"""
Dimension State Store for PySpark CDC Consumer
Keyed dict with TTL for managing dimension table state (CUSTOMERS, PRODUCTS, ORDER_ITEMS)
Supports bootstrap from PostgreSQL on startup for crash recovery.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import psycopg2

logger = logging.getLogger(__name__)


@dataclass
class StateEntry:
    """Single entry in the state store"""
    data: Dict[str, Any]
    updated_at: float  # epoch seconds


class DimensionStateStore:
    """
    Keyed state store for dimension tables.

    Features:
    - O(1) lookup/update by key
    - Auto-dedup (latest value per key)
    - TTL-based expiry
    - Bootstrap from PostgreSQL on startup
    """

    def __init__(self, name: str, ttl_seconds: int = 3600):
        self.name = name
        self.ttl_seconds = ttl_seconds
        self._state: Dict[Any, StateEntry] = {}
        self._updates_since_expire = 0

    def put(self, key: Any, data: Dict[str, Any]) -> None:
        """Upsert a record by key. Overwrites previous value."""
        self._state[key] = StateEntry(data=data, updated_at=time.time())
        self._updates_since_expire += 1
        # Periodic expiry check every 500 updates
        if self._updates_since_expire >= 500:
            self.expire()
            self._updates_since_expire = 0

    def get(self, key: Any) -> Optional[Dict[str, Any]]:
        """Get record by key, returns None if not found or expired."""
        entry = self._state.get(key)
        if entry is None:
            return None
        if time.time() - entry.updated_at > self.ttl_seconds:
            del self._state[key]
            return None
        return entry.data

    def get_all(self) -> Dict[Any, Dict[str, Any]]:
        """Get all non-expired entries as {key: data}."""
        now = time.time()
        result = {}
        expired_keys = []
        for key, entry in self._state.items():
            if now - entry.updated_at > self.ttl_seconds:
                expired_keys.append(key)
            else:
                result[key] = entry.data
        for k in expired_keys:
            del self._state[k]
        return result

    def get_all_values(self) -> List[Dict[str, Any]]:
        """Get all non-expired values as a list (for DataFrame creation)."""
        return list(self.get_all().values())

    def remove(self, key: Any) -> None:
        """Remove a specific key."""
        self._state.pop(key, None)

    def expire(self) -> int:
        """Remove expired entries. Returns count of removed entries."""
        now = time.time()
        expired = [k for k, v in self._state.items()
                   if now - v.updated_at > self.ttl_seconds]
        for k in expired:
            del self._state[k]
        if expired:
            logger.debug(f"State '{self.name}': expired {len(expired)} entries")
        return len(expired)

    def size(self) -> int:
        return len(self._state)

    def clear(self) -> None:
        self._state.clear()


class OrderItemsStateStore:
    """
    Specialized state store for ORDER_ITEMS.
    Groups items by order_id for aggregation during join.
    """

    def __init__(self, ttl_seconds: int = 3600):
        self.ttl_seconds = ttl_seconds
        self._state: Dict[int, StateEntry] = {}  # order_id -> StateEntry(data=list of items)
        self._updates_since_expire = 0

    def add_item(self, order_id: int, item: Dict[str, Any]) -> None:
        """Add an item to the order's item list."""
        now = time.time()
        entry = self._state.get(order_id)
        if entry is None or now - entry.updated_at > self.ttl_seconds:
            self._state[order_id] = StateEntry(data=[item], updated_at=now)
        else:
            entry.data.append(item)
            entry.updated_at = now
            # Bound per-order items
            if len(entry.data) > 100:
                entry.data = entry.data[-50:]

        self._updates_since_expire += 1
        if self._updates_since_expire >= 500:
            self.expire()
            self._updates_since_expire = 0

    def get_items(self, order_id: int) -> List[Dict[str, Any]]:
        """Get all items for an order."""
        entry = self._state.get(order_id)
        if entry is None:
            return []
        if time.time() - entry.updated_at > self.ttl_seconds:
            del self._state[order_id]
            return []
        return entry.data

    def get_item_count(self, order_id: int) -> int:
        return len(self.get_items(order_id))

    def get_all_items_flat(self) -> List[Dict[str, Any]]:
        """Get all items as flat list (for DataFrame creation)."""
        now = time.time()
        result = []
        expired = []
        for order_id, entry in self._state.items():
            if now - entry.updated_at > self.ttl_seconds:
                expired.append(order_id)
            else:
                result.extend(entry.data)
        for k in expired:
            del self._state[k]
        return result

    def expire(self) -> int:
        now = time.time()
        expired = [k for k, v in self._state.items()
                   if now - v.updated_at > self.ttl_seconds]
        for k in expired:
            del self._state[k]
        return len(expired)

    def size(self) -> int:
        """Number of unique order_ids."""
        return len(self._state)

    def total_items(self) -> int:
        """Total items across all orders."""
        return sum(len(e.data) for e in self._state.values())

    def clear(self) -> None:
        self._state.clear()


def bootstrap_dimension_state(
    conn,
    customer_store: DimensionStateStore,
    product_store: DimensionStateStore,
    order_items_store: OrderItemsStateStore,
) -> Dict[str, int]:
    """
    Bootstrap dimension state from PostgreSQL on startup.
    Loads latest records from cdc_customers, cdc_products, and cdc_order_items.

    Returns dict with counts of loaded records per table.
    """
    stats = {"customers": 0, "products": 0, "order_items": 0}

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Load customers (latest per customer_id)
            cur.execute("""
                SELECT DISTINCT ON (customer_id)
                    customer_id, name, email, tier, region, created_at
                FROM cdc_customers
                ORDER BY customer_id, source_ts DESC NULLS LAST
            """)
            for row in cur:
                customer_store.put(row["customer_id"], {
                    "CUSTOMER_ID": row["customer_id"],
                    "FIRST_NAME": (row.get("name") or "").split(" ")[0],
                    "LAST_NAME": " ".join((row.get("name") or "").split(" ")[1:]),
                    "EMAIL": row.get("email"),
                    "TIER": row.get("tier"),
                    "CITY": row.get("region"),
                    "CREATED_AT": str(row.get("created_at", "")),
                })
                stats["customers"] += 1

            # Load products (latest per product_id)
            cur.execute("""
                SELECT DISTINCT ON (product_id)
                    product_id, name, category, price, stock_qty, is_active
                FROM cdc_products
                ORDER BY product_id, source_ts DESC NULLS LAST
            """)
            for row in cur:
                product_store.put(row["product_id"], {
                    "PRODUCT_ID": row["product_id"],
                    "NAME": row.get("name"),
                    "CATEGORY": row.get("category"),
                    "PRICE": row.get("price"),
                    "STOCK_QUANTITY": row.get("stock_qty"),
                    "STATUS": "ACTIVE" if row.get("is_active") else "INACTIVE",
                })
                stats["products"] += 1

            # Load recent order items (last 1 hour)
            cur.execute("""
                SELECT item_id, order_id, product_id, quantity,
                       unit_price, subtotal
                FROM cdc_order_items
                WHERE source_ts >= NOW() - INTERVAL '1 hour'
                   OR source_ts IS NULL
                ORDER BY order_id, item_id
            """)
            for row in cur:
                order_id = row.get("order_id")
                if order_id is not None:
                    order_items_store.add_item(order_id, {
                        "ITEM_ID": row.get("item_id"),
                        "ORDER_ID": order_id,
                        "PRODUCT_ID": row.get("product_id"),
                        "QUANTITY": row.get("quantity"),
                        "UNIT_PRICE": row.get("unit_price"),
                        "SUBTOTAL": row.get("subtotal"),
                    })
                    stats["order_items"] += 1

        logger.info(
            f"State bootstrap complete: "
            f"{stats['customers']} customers, "
            f"{stats['products']} products, "
            f"{stats['order_items']} order items"
        )

    except psycopg2.Error as e:
        logger.warning(f"State bootstrap failed (will start with empty state): {e}")
    except Exception as e:
        logger.warning(f"State bootstrap error: {e}")

    return stats
