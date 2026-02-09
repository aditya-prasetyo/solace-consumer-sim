"""
In-Memory Storage for REST API Mock Server
Thread-safe storage with TTL and max size limits
"""

import threading
import time
import uuid
from collections import OrderedDict
from typing import Any, Dict, List, Optional


class InMemoryStore:
    """Thread-safe in-memory event storage with TTL"""

    def __init__(self, max_size: int = 10000, ttl_seconds: int = 3600):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._data: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self._lock = threading.Lock()

    def add(self, event: Dict[str, Any], event_id: Optional[str] = None) -> str:
        """Add event to store, returns event_id"""
        eid = event_id or str(uuid.uuid4())
        event["_id"] = eid
        event["_stored_at"] = time.time()

        with self._lock:
            # Evict oldest if full
            while len(self._data) >= self.max_size:
                self._data.popitem(last=False)

            self._data[eid] = event

        return eid

    def get(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get event by ID"""
        with self._lock:
            event = self._data.get(event_id)
            if event and self._is_expired(event):
                del self._data[event_id]
                return None
            return event

    def get_all(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get all events with pagination"""
        with self._lock:
            self._cleanup_expired()
            items = list(self._data.values())
            return items[offset:offset + limit]

    def get_by_filter(self, key: str, value: Any, limit: int = 100) -> List[Dict[str, Any]]:
        """Get events matching a filter"""
        with self._lock:
            results = []
            for event in self._data.values():
                if event.get(key) == value and not self._is_expired(event):
                    results.append(event)
                    if len(results) >= limit:
                        break
            return results

    def count(self) -> int:
        """Count active events"""
        with self._lock:
            self._cleanup_expired()
            return len(self._data)

    def clear(self) -> None:
        """Clear all events"""
        with self._lock:
            self._data.clear()

    def _is_expired(self, event: Dict[str, Any]) -> bool:
        """Check if event is expired"""
        stored_at = event.get("_stored_at", 0)
        return (time.time() - stored_at) > self.ttl_seconds

    def _cleanup_expired(self) -> None:
        """Remove expired events"""
        expired = [
            eid for eid, event in self._data.items()
            if self._is_expired(event)
        ]
        for eid in expired:
            del self._data[eid]


class EventStoreManager:
    """Manages multiple event stores and metrics"""

    def __init__(
        self,
        events_max: int = 10000,
        events_ttl: int = 3600,
        notifications_max: int = 1000,
        notifications_ttl: int = 3600
    ):
        self.events = InMemoryStore(events_max, events_ttl)
        self.enriched_orders = InMemoryStore(events_max, events_ttl)
        self.extracted_items = InMemoryStore(events_max, events_ttl)
        self.notifications = InMemoryStore(notifications_max, notifications_ttl)

        # Metrics
        self._total_events = 0
        self._events_by_table: Dict[str, int] = {}
        self._events_by_op: Dict[str, int] = {}
        self._errors = 0
        self._start_time = time.time()
        self._lock = threading.Lock()

    def record_event(self, table: str, operation: str) -> None:
        """Record event metrics"""
        with self._lock:
            self._total_events += 1
            self._events_by_table[table] = self._events_by_table.get(table, 0) + 1
            self._events_by_op[operation] = self._events_by_op.get(operation, 0) + 1

    def record_error(self) -> None:
        """Record error"""
        with self._lock:
            self._errors += 1

    def get_metrics(self) -> Dict[str, Any]:
        """Get all metrics"""
        with self._lock:
            return {
                "total_events": self._total_events,
                "events_by_table": dict(self._events_by_table),
                "events_by_operation": dict(self._events_by_op),
                "enriched_orders": self.enriched_orders.count(),
                "extracted_items": self.extracted_items.count(),
                "notifications": self.notifications.count(),
                "errors": self._errors,
                "uptime_seconds": round(time.time() - self._start_time, 2)
            }
