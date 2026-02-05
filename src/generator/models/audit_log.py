"""
Audit Log Table Data Generator
"""

import json
import random
from datetime import datetime
from typing import Any, Dict
from faker import Faker

from .base import BaseTableGenerator


fake = Faker()


class AuditLogGenerator(BaseTableGenerator):
    """Generator for AUDIT_LOG table - Insert only"""

    ACTIONS = [
        "USER_LOGIN", "USER_LOGOUT", "ORDER_CREATED", "ORDER_UPDATED",
        "PAYMENT_PROCESSED", "REFUND_ISSUED", "INVENTORY_ADJUSTED",
        "PRICE_CHANGED", "CUSTOMER_UPDATED", "REPORT_GENERATED"
    ]

    ENTITIES = ["ORDER", "CUSTOMER", "PRODUCT", "PAYMENT", "USER", "REPORT"]

    def __init__(self):
        super().__init__("AUDIT_LOG")

    def generate_insert(self) -> Dict[str, Any]:
        """Generate new audit log entry"""
        log_id = self._next_id()
        now = datetime.now()
        action = random.choice(self.ACTIONS)

        # Generate context-appropriate details
        details = self._generate_details(action)

        data = {
            "LOG_ID": log_id,
            "ACTION": action,
            "ENTITY_TYPE": self._get_entity_type(action),
            "ENTITY_ID": random.randint(1, 10000),
            "USER_ID": random.randint(1, 100),
            "DETAILS": json.dumps(details),
            "IP_ADDRESS": fake.ipv4(),
            "CREATED_AT": now.isoformat()
        }

        # Audit logs are typically not updated or deleted
        # We don't store them for future modification
        return data

    def _get_entity_type(self, action: str) -> str:
        """Derive entity type from action"""
        if "ORDER" in action:
            return "ORDER"
        elif "USER" in action:
            return "USER"
        elif "PAYMENT" in action or "REFUND" in action:
            return "PAYMENT"
        elif "INVENTORY" in action or "PRICE" in action:
            return "PRODUCT"
        elif "CUSTOMER" in action:
            return "CUSTOMER"
        elif "REPORT" in action:
            return "REPORT"
        return random.choice(self.ENTITIES)

    def _generate_details(self, action: str) -> Dict[str, Any]:
        """Generate action-specific details"""
        if action == "USER_LOGIN":
            return {
                "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
                "os": random.choice(["Windows", "MacOS", "Linux", "iOS", "Android"]),
                "success": random.random() > 0.1
            }
        elif action == "ORDER_CREATED":
            return {
                "items_count": random.randint(1, 10),
                "total_amount": round(random.uniform(50, 5000), 2),
                "payment_method": random.choice(["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"])
            }
        elif action == "PAYMENT_PROCESSED":
            return {
                "amount": round(random.uniform(50, 5000), 2),
                "currency": "USD",
                "processor": random.choice(["Stripe", "PayPal", "Square"]),
                "status": random.choice(["SUCCESS", "PENDING", "FAILED"])
            }
        elif action == "PRICE_CHANGED":
            old_price = round(random.uniform(10, 500), 2)
            return {
                "old_price": old_price,
                "new_price": round(old_price * random.uniform(0.8, 1.3), 2),
                "reason": random.choice(["PROMOTION", "COST_ADJUSTMENT", "MARKET_PRICE"])
            }
        elif action == "INVENTORY_ADJUSTED":
            return {
                "adjustment": random.randint(-100, 500),
                "reason": random.choice(["RESTOCK", "SALE", "DAMAGE", "RETURN", "AUDIT"])
            }
        else:
            return {"note": fake.sentence()}

    def _modify_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Audit logs should not be modified - return unchanged"""
        # This should never be called per blueprint (100% insert)
        return record

    def generate_update(self) -> Dict[str, Any] | None:
        """Override: Audit logs don't support updates"""
        return None

    def generate_delete(self) -> int | None:
        """Override: Audit logs don't support deletes"""
        return None
