"""
REST API Sink for PySpark CDC Consumer
Pushes processed CDC events to REST API target server
"""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from ..config import ErrorHandlingConfig

logger = logging.getLogger(__name__)


class RestApiSink:
    """Push CDC events to REST API target"""

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        error_config: Optional[ErrorHandlingConfig] = None,
        consumer_id: str = "spark-consumer-1",
        timeout: int = 10,
    ):
        self.base_url = base_url.rstrip("/")
        self.error_config = error_config or ErrorHandlingConfig()
        self.consumer_id = consumer_id
        self.timeout = timeout

        # Session with connection pooling and retry
        self._session = self._create_session()

        # Circuit breaker state
        self._failure_count = 0
        self._circuit_open = False
        self._circuit_open_time = 0.0

        # Stats
        self._events_sent = 0
        self._events_failed = 0
        self._batches_sent = 0

    def _create_session(self) -> requests.Session:
        """Create requests session with retry adapter"""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.error_config.retry.max_attempts,
            backoff_factor=self.error_config.retry.multiplier,
            status_forcelist=[502, 503, 504],
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=5,
            pool_maxsize=10,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({"Content-Type": "application/json"})
        return session

    def _check_circuit(self) -> bool:
        """Check if circuit breaker allows operations"""
        if not self._circuit_open:
            return True
        elapsed = time.time() - self._circuit_open_time
        if elapsed >= self.error_config.circuit_breaker.recovery_timeout:
            logger.info("REST sink circuit breaker: attempting recovery")
            self._circuit_open = False
            self._failure_count = 0
            return True
        logger.warning(
            f"REST sink circuit breaker OPEN, "
            f"{self.error_config.circuit_breaker.recovery_timeout - elapsed:.0f}s until retry"
        )
        return False

    def _record_failure(self) -> None:
        self._failure_count += 1
        self._events_failed += 1
        if self._failure_count >= self.error_config.circuit_breaker.failure_threshold:
            self._circuit_open = True
            self._circuit_open_time = time.time()
            logger.error(f"REST sink circuit breaker OPENED after {self._failure_count} failures")

    def _record_success(self) -> None:
        self._failure_count = 0

    # ---- Publish Methods ----

    def send_events_batch(
        self, table: str, events: List[Dict[str, Any]]
    ) -> bool:
        """Send batch of CDC events to REST API"""
        if not events or not self._check_circuit():
            return False

        payload = {
            "events": [
                {
                    "table": table,
                    "operation": e.get("operation", "c"),
                    "data": {k: v for k, v in e.items()
                             if not k.startswith("_") and k not in ("operation", "source_ts", "processed_at")},
                    "source_ts": e.get("source_ts"),
                    "consumer_id": self.consumer_id,
                }
                for e in events
            ]
        }

        return self._post(f"{self.base_url}/api/v1/events/batch", payload)

    def send_enriched_orders(
        self, orders: List[Dict[str, Any]]
    ) -> bool:
        """Send enriched orders (3-way join result) to REST API"""
        if not orders or not self._check_circuit():
            return False

        payload = {
            "orders": [
                {
                    "order_id": o.get("order_id", 0),
                    "customer_id": o.get("customer_id"),
                    "customer_name": o.get("customer_name"),
                    "customer_tier": o.get("customer_tier"),
                    "customer_region": o.get("customer_region"),
                    "total_amount": o.get("total_amount"),
                    "item_count": o.get("item_count"),
                    "order_date": _serialize_dt(o.get("order_date")),
                    "status": o.get("status"),
                    "window_start": _serialize_dt(o.get("window_start")),
                    "window_end": _serialize_dt(o.get("window_end")),
                }
                for o in orders
            ]
        }

        return self._post(f"{self.base_url}/api/v1/enriched-orders/batch", payload)

    def send_extracted_items(
        self, items: List[Dict[str, Any]]
    ) -> bool:
        """Send XML-extracted items to REST API"""
        if not items or not self._check_circuit():
            return False

        payload = [
            {
                "order_id": i.get("order_id", 0),
                "product_id": i.get("product_id"),
                "quantity": i.get("quantity"),
                "unit_price": i.get("unit_price"),
                "subtotal": i.get("subtotal"),
                "extracted_from": i.get("extracted_from", "SHIPPING_INFO_XML"),
            }
            for i in items
        ]

        return self._post(f"{self.base_url}/api/v1/order-items/batch", payload)

    def send_notification(
        self,
        event_type: str,
        message: str,
        severity: str = "info",
        payload: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Send notification to REST API"""
        data = {
            "event_type": event_type,
            "message": message,
            "severity": severity,
            "payload": payload,
        }
        return self._post(f"{self.base_url}/api/v1/notifications", data)

    def health_check(self) -> bool:
        """Check if REST API is healthy"""
        try:
            resp = self._session.get(
                f"{self.base_url}/health", timeout=self.timeout
            )
            return resp.status_code == 200
        except Exception:
            return False

    # ---- Internal ----

    def _post(self, url: str, payload: Any) -> bool:
        """POST data to REST API with error handling"""
        try:
            resp = self._session.post(
                url,
                json=payload,
                timeout=self.timeout,
            )

            if resp.status_code in (200, 201, 202):
                self._record_success()
                self._events_sent += 1
                self._batches_sent += 1
                logger.debug(f"REST POST OK: {url} -> {resp.status_code}")
                return True
            else:
                logger.warning(f"REST POST failed: {url} -> {resp.status_code}: {resp.text[:200]}")
                self._record_failure()
                return False

        except requests.exceptions.ConnectionError as e:
            logger.error(f"REST connection error: {e}")
            self._record_failure()
            return False
        except requests.exceptions.Timeout as e:
            logger.error(f"REST timeout: {e}")
            self._record_failure()
            return False
        except Exception as e:
            logger.error(f"REST POST error: {e}")
            self._record_failure()
            return False

    def get_stats(self) -> Dict[str, Any]:
        """Get sink statistics"""
        return {
            "events_sent": self._events_sent,
            "events_failed": self._events_failed,
            "batches_sent": self._batches_sent,
            "circuit_open": self._circuit_open,
            "failure_count": self._failure_count,
            "base_url": self.base_url,
        }

    def close(self) -> None:
        """Close session"""
        if self._session:
            self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def _serialize_dt(val) -> Optional[str]:
    """Serialize datetime to ISO string"""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.isoformat()
    return str(val)
