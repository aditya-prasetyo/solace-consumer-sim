"""
Order Table Data Generator
"""

import random
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from faker import Faker

from .base import BaseTableGenerator


fake = Faker()


class OrderGenerator(BaseTableGenerator):
    """Generator for ORDERS table"""

    STATUSES = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]

    # Schema evolution: extra columns that may appear (simulating Oracle DDL changes)
    EXTRA_COLUMNS_POOL = [
        ("DISCOUNT_CODE", lambda: fake.bothify("???-####").upper()),
        ("DISCOUNT_PERCENT", lambda: round(random.uniform(5, 30), 1)),
        ("LOYALTY_POINTS", lambda: random.randint(10, 5000)),
        ("SALES_CHANNEL", lambda: random.choice(["WEB", "MOBILE", "POS", "API"])),
        ("PRIORITY", lambda: random.choice(["LOW", "NORMAL", "HIGH", "URGENT"])),
    ]

    def __init__(
        self,
        xml_enabled: bool = True,
        xml_probability: float = 0.3,
        schema_evolution: bool = False,
        evolution_probability: float = 0.15,
    ):
        super().__init__("ORDERS")
        self.xml_enabled = xml_enabled
        self.xml_probability = xml_probability
        self.schema_evolution = schema_evolution
        self.evolution_probability = evolution_probability
        self._customer_ids = list(range(1, 1001))  # Pre-generated customer IDs

    def generate_insert(self) -> Dict[str, Any]:
        """Generate new order data"""
        order_id = self._next_id()
        now = datetime.now()

        data = {
            "ORDER_ID": order_id,
            "CUSTOMER_ID": random.choice(self._customer_ids),
            "ORDER_DATE": now.isoformat(),
            "STATUS": "PENDING",
            "TOTAL_AMOUNT": round(random.uniform(50, 5000), 2),
            "CREATED_AT": now.isoformat(),
            "UPDATED_AT": now.isoformat()
        }

        # Add XML shipping info occasionally
        if self.xml_enabled and random.random() < self.xml_probability:
            data["SHIPPING_INFO"] = self._generate_shipping_xml()

        # Schema evolution: add extra columns occasionally
        if self.schema_evolution and random.random() < self.evolution_probability:
            extras = random.sample(
                self.EXTRA_COLUMNS_POOL,
                k=random.randint(1, min(3, len(self.EXTRA_COLUMNS_POOL)))
            )
            for col_name, gen_fn in extras:
                data[col_name] = gen_fn()

        self._store_record(order_id, data)
        return data

    def _modify_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Modify order for UPDATE"""
        # Update status progression
        current_status = record.get("STATUS", "PENDING")
        status_idx = self.STATUSES.index(current_status) if current_status in self.STATUSES else 0

        if status_idx < len(self.STATUSES) - 1 and random.random() > 0.3:
            record["STATUS"] = self.STATUSES[status_idx + 1]

        # Occasionally update amount (corrections)
        if random.random() < 0.2:
            record["TOTAL_AMOUNT"] = round(record.get("TOTAL_AMOUNT", 100) * random.uniform(0.9, 1.1), 2)

        record["UPDATED_AT"] = datetime.now().isoformat()
        return record

    def _generate_shipping_xml(self) -> str:
        """Generate XML shipping information"""
        num_items = random.randint(1, 5)
        items_xml = []

        for i in range(num_items):
            product_id = random.randint(1, 500)
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(10, 500), 2)
            items_xml.append(
                f'    <item product_id="{product_id}" quantity="{quantity}" unit_price="{unit_price}" />'
            )

        shipping_method = random.choice(["STANDARD", "EXPRESS", "OVERNIGHT"])
        address = fake.address().replace("\n", ", ")

        xml = f"""<shipping_info>
  <items>
{chr(10).join(items_xml)}
  </items>
  <shipping>
    <method>{shipping_method}</method>
    <address>{address}</address>
    <estimated_days>{random.randint(1, 7)}</estimated_days>
  </shipping>
</shipping_info>"""
        return xml
