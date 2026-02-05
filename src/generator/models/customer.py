"""
Customer Table Data Generator
"""

import random
from datetime import datetime
from typing import Any, Dict
from faker import Faker

from .base import BaseTableGenerator


fake = Faker()


class CustomerGenerator(BaseTableGenerator):
    """Generator for CUSTOMERS table"""

    TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]

    def __init__(self):
        super().__init__("CUSTOMERS")

    def generate_insert(self) -> Dict[str, Any]:
        """Generate new customer data"""
        customer_id = self._next_id()
        now = datetime.now()

        data = {
            "CUSTOMER_ID": customer_id,
            "FIRST_NAME": fake.first_name(),
            "LAST_NAME": fake.last_name(),
            "EMAIL": fake.email(),
            "PHONE": fake.phone_number()[:20],  # Truncate to fit typical field size
            "ADDRESS": fake.address().replace("\n", ", "),
            "CITY": fake.city(),
            "COUNTRY": fake.country_code(),
            "TIER": random.choices(self.TIERS, weights=[50, 30, 15, 5])[0],
            "CREATED_AT": now.isoformat(),
            "UPDATED_AT": now.isoformat()
        }

        self._store_record(customer_id, data)
        return data

    def _modify_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Modify customer for UPDATE"""
        # Update contact info
        if random.random() < 0.3:
            record["EMAIL"] = fake.email()

        if random.random() < 0.2:
            record["PHONE"] = fake.phone_number()[:20]

        # Address change
        if random.random() < 0.15:
            record["ADDRESS"] = fake.address().replace("\n", ", ")
            record["CITY"] = fake.city()

        # Tier upgrade/downgrade
        if random.random() < 0.1:
            current_tier = record.get("TIER", "BRONZE")
            current_idx = self.TIERS.index(current_tier) if current_tier in self.TIERS else 0

            # 70% chance upgrade, 30% downgrade
            if random.random() < 0.7 and current_idx < len(self.TIERS) - 1:
                record["TIER"] = self.TIERS[current_idx + 1]
            elif current_idx > 0:
                record["TIER"] = self.TIERS[current_idx - 1]

        record["UPDATED_AT"] = datetime.now().isoformat()
        return record
