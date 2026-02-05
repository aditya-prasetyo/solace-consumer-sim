"""
Product Table Data Generator
"""

import random
from datetime import datetime
from typing import Any, Dict
from faker import Faker

from .base import BaseTableGenerator


fake = Faker()


class ProductGenerator(BaseTableGenerator):
    """Generator for PRODUCTS table"""

    CATEGORIES = [
        "Electronics", "Clothing", "Home & Garden", "Sports",
        "Books", "Toys", "Health", "Automotive", "Food", "Beauty"
    ]

    STATUSES = ["ACTIVE", "INACTIVE", "DISCONTINUED"]

    def __init__(self):
        super().__init__("PRODUCTS")

    def generate_insert(self) -> Dict[str, Any]:
        """Generate new product data"""
        product_id = self._next_id()
        now = datetime.now()

        base_price = round(random.uniform(10, 1000), 2)

        data = {
            "PRODUCT_ID": product_id,
            "NAME": fake.catch_phrase(),
            "DESCRIPTION": fake.text(max_nb_chars=200),
            "CATEGORY": random.choice(self.CATEGORIES),
            "PRICE": base_price,
            "STOCK_QUANTITY": random.randint(0, 1000),
            "STATUS": "ACTIVE",
            "CREATED_AT": now.isoformat(),
            "UPDATED_AT": now.isoformat()
        }

        self._store_record(product_id, data)
        return data

    def _modify_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Modify product for UPDATE"""
        # Price change (most common)
        if random.random() < 0.4:
            current_price = record.get("PRICE", 100)
            # Price fluctuation -20% to +30%
            record["PRICE"] = round(current_price * random.uniform(0.8, 1.3), 2)

        # Stock update
        if random.random() < 0.5:
            current_stock = record.get("STOCK_QUANTITY", 100)
            # Restock or sales reduction
            if random.random() < 0.3:
                # Restock
                record["STOCK_QUANTITY"] = current_stock + random.randint(50, 500)
            else:
                # Sales
                record["STOCK_QUANTITY"] = max(0, current_stock - random.randint(1, 50))

        # Description update
        if random.random() < 0.1:
            record["DESCRIPTION"] = fake.text(max_nb_chars=200)

        # Status change
        if random.random() < 0.05:
            current_status = record.get("STATUS", "ACTIVE")
            if current_status == "ACTIVE":
                record["STATUS"] = random.choice(["INACTIVE", "DISCONTINUED"])
            elif current_status == "INACTIVE":
                record["STATUS"] = "ACTIVE"

        record["UPDATED_AT"] = datetime.now().isoformat()
        return record
