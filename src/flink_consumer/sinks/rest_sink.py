"""
REST API Sink for PyFlink CDC Consumer
Pushes processed CDC events to REST API target with circuit breaker
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..config import RestApiSinkConfig, ErrorHandlingConfig

logger = logging.getLogger(__name__)


class FlinkRestApiSink:
    """Push CDC events to REST API target"""

    def __init__(self, config: RestApiSinkConfig, error_config: ErrorHandlingConfig):
        self.config = config
        self.error_config = error_config
        self._session = self._create_session()

        # Circuit breaker
        self._failure_count = 0
        self._circuit_open = False
        self._circuit_open_time = 0.0

        # Stats
        self._sent = 0
        self._failed = 0

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.config.retry_attempts,
            backoff_factor=1,
            status_forcelist=[502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=5, pool_maxsize=10)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({"Content-Type": "application/json"})
        return session

    def _check_circuit(self) -> bool:
        if not self._circuit_open:
            return True
        if time.time() - self._circuit_open_time >= self.error_config.circuit_breaker.recovery_timeout:
            self._circuit_open = False
            self._failure_count = 0
            return True
        return False

    def _record_failure(self) -> None:
        self._failure_count += 1
        self._failed += 1
        if self._failure_count >= self.error_config.circuit_breaker.failure_threshold:
            self._circuit_open = True
            self._circuit_open_time = time.time()
            logger.error("REST sink circuit breaker OPENED")

    def _record_success(self) -> None:
        self._failure_count = 0
        self._sent += 1

    def send_events_batch(self, table: str, events: List[Dict[str, Any]]) -> bool:
        """Send batch of CDC events"""
        if not events or not self._check_circuit():
            return False

        payload = {
            "events": [
                {
                    "table": table,
                    "operation": e.get("_cdc_op", e.get("operation", "c")),
                    "data": {k: v for k, v in e.items() if not k.startswith("_")},
                    "source_ts": e.get("_source_ts"),
                    "consumer_id": self.config.consumer_id,
                }
                for e in events
            ]
        }
        return self._post(f"{self.config.url}/api/v1/events/batch", payload)

    def send_enriched_orders(self, orders: List[Dict[str, Any]]) -> bool:
        """Send enriched orders (3-way join result)"""
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
                    "order_date": _dt_str(o.get("order_date")),
                    "status": o.get("status"),
                    "window_start": _dt_str(o.get("window_start")),
                    "window_end": _dt_str(o.get("window_end")),
                }
                for o in orders
            ]
        }
        return self._post(f"{self.config.url}/api/v1/enriched-orders/batch", payload)

    def send_extracted_items(self, items: List[Dict[str, Any]]) -> bool:
        """Send XML-extracted items"""
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
        return self._post(f"{self.config.url}/api/v1/order-items/batch", payload)

    def send_notification(
        self, event_type: str, message: str,
        severity: str = "info", payload: Optional[Dict] = None
    ) -> bool:
        data = {
            "event_type": event_type,
            "message": message,
            "severity": severity,
            "payload": payload,
        }
        return self._post(f"{self.config.url}/api/v1/notifications", data)

    def health_check(self) -> bool:
        try:
            r = self._session.get(f"{self.config.url}/health", timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def _post(self, url: str, payload: Any) -> bool:
        try:
            r = self._session.post(url, json=payload, timeout=self.config.timeout)
            if r.status_code in (200, 201, 202):
                self._record_success()
                return True
            else:
                logger.warning(f"REST POST {url} -> {r.status_code}")
                self._record_failure()
                return False
        except requests.exceptions.ConnectionError:
            self._record_failure()
            return False
        except requests.exceptions.Timeout:
            self._record_failure()
            return False
        except Exception as e:
            logger.error(f"REST error: {e}")
            self._record_failure()
            return False

    def get_stats(self) -> Dict[str, Any]:
        return {
            "sent": self._sent,
            "failed": self._failed,
            "circuit_open": self._circuit_open,
            "base_url": self.config.url,
        }

    def close(self) -> None:
        if self._session:
            self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _dt_str(val) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.isoformat()
    return str(val)
