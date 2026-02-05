"""
Order Item Table Data Generator
"""

import random
from datetime import datetime
from typing import Any, Dict

from .base import BaseTableGenerator


class OrderItemGenerator(BaseTableGenerator):
    """Generator for ORDER_ITEMS table"""

    def __init__(self):
        super().__init__("ORDER_ITEMS")
        self._order_ids: list[int] = []  # Track order IDs for referential integrity
        self._product_ids = list(range(1, 501))  # Pre-generated product IDs

    def set_order_ids(self, order_ids: list[int]) -> None:
        """Set available order IDs for foreign key reference"""
        self._order_ids = order_ids

    def add_order_id(self, order_id: int) -> None:
        """Add new order ID to available pool"""
        self._order_ids.append(order_id)
        # Keep list bounded
        if len(self._order_ids) > 10000:
            self._order_ids = self._order_ids[-5000:]

    def generate_insert(self) -> Dict[str, Any]:
        """Generate new order item data"""
        if not self._order_ids:
            # Fallback if no orders exist yet
            self._order_ids = list(range(1, 100))

        item_id = self._next_id()
        now = datetime.now()
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(10, 500), 2)

        data = {
            "ITEM_ID": item_id,
            "ORDER_ID": random.choice(self._order_ids),
            "PRODUCT_ID": random.choice(self._product_ids),
            "QUANTITY": quantity,
            "UNIT_PRICE": unit_price,
            "SUBTOTAL": round(quantity * unit_price, 2),
            "CREATED_AT": now.isoformat(),
            "UPDATED_AT": now.isoformat()
        }

        self._store_record(item_id, data)
        return data

    def _modify_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Modify order item for UPDATE"""
        # Update quantity (order adjustment)
        if random.random() < 0.5:
            new_quantity = random.randint(1, 10)
            record["QUANTITY"] = new_quantity
            unit_price = record.get("UNIT_PRICE", 100)
            record["SUBTOTAL"] = round(new_quantity * unit_price, 2)

        # Price adjustment (rare)
        if random.random() < 0.1:
            new_price = round(record.get("UNIT_PRICE", 100) * random.uniform(0.9, 1.1), 2)
            record["UNIT_PRICE"] = new_price
            record["SUBTOTAL"] = round(record.get("QUANTITY", 1) * new_price, 2)

        record["UPDATED_AT"] = datetime.now().isoformat()
        return record
