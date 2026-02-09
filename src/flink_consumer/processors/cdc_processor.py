"""
CDC Event Processor for PyFlink
Handles parsing, deduplication, and operation routing
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Table primary key mapping
TABLE_KEY_MAP = {
    "ORDERS": "ORDER_ID",
    "ORDER_ITEMS": "ITEM_ID",
    "CUSTOMERS": "CUSTOMER_ID",
    "PRODUCTS": "PRODUCT_ID",
    "AUDIT_LOG": "LOG_ID",
}


class CDCEventParser:
    """Parse and normalize CDC events for Flink processing"""

    def parse(self, payload: Dict[str, Any], table: str) -> Optional[Dict[str, Any]]:
        """
        Parse a CDC event payload into a normalized record.
        Returns None if the event is invalid.
        """
        op = payload.get("op", "c")
        ts_ms = payload.get("ts_ms", int(datetime.now().timestamp() * 1000))
        source = payload.get("source", {})

        # Get data based on operation
        if op in ("c", "r"):  # create or snapshot
            data = payload.get("after", {})
        elif op == "u":  # update
            data = payload.get("after", {})
        elif op == "d":  # delete
            data = payload.get("before", {})
        else:
            return None

        if not data:
            return None

        # Add CDC metadata
        data["_cdc_op"] = op
        data["_cdc_ts_ms"] = ts_ms
        data["_cdc_table"] = source.get("table", table)
        data["_cdc_scn"] = source.get("scn", "")
        data["_source_ts"] = datetime.fromtimestamp(ts_ms / 1000).isoformat()

        return data

    def parse_raw_json(self, json_str: str, table: str) -> Optional[Dict[str, Any]]:
        """Parse raw JSON string CDC event"""
        try:
            event = json.loads(json_str)
            payload = event.get("payload", event)
            return self.parse(payload, table)
        except json.JSONDecodeError:
            return None


class Deduplicator:
    """Deduplicate CDC events by key within a window"""

    def __init__(self, max_entries: int = 10000):
        self._seen: Dict[str, int] = {}  # key -> timestamp
        self._max_entries = max_entries

    def is_duplicate(self, table: str, record: Dict[str, Any]) -> bool:
        """Check if record is a duplicate (same key, older or equal timestamp)"""
        key_col = TABLE_KEY_MAP.get(table)
        if not key_col or key_col not in record:
            return False

        key = f"{table}:{record[key_col]}"
        ts = record.get("_cdc_ts_ms", 0)

        if key in self._seen and self._seen[key] >= ts:
            return True

        self._seen[key] = ts

        # Evict old entries
        if len(self._seen) > self._max_entries:
            sorted_keys = sorted(self._seen.items(), key=lambda x: x[1])
            for k, _ in sorted_keys[:len(sorted_keys) // 2]:
                del self._seen[k]

        return False

    def deduplicate_batch(
        self, table: str, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Deduplicate a batch, keeping latest per key"""
        key_col = TABLE_KEY_MAP.get(table)
        if not key_col:
            return records

        # Group by key, keep latest
        latest: Dict[Any, Dict[str, Any]] = {}
        for r in records:
            key = r.get(key_col)
            if key is None:
                continue
            existing = latest.get(key)
            if existing is None or r.get("_cdc_ts_ms", 0) > existing.get("_cdc_ts_ms", 0):
                latest[key] = r

        return list(latest.values())
