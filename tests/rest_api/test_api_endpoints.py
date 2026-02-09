"""
Tests for REST API Endpoints (FastAPI)
Covers: health, events, enriched orders, extracted items, notifications, batch endpoints
"""

import pytest
from fastapi.testclient import TestClient

from src.rest_api.main import app
from src.rest_api.storage import EventStoreManager
from src.rest_api.routes.events import set_store


@pytest.fixture(autouse=True)
def fresh_store():
    """Reset store before each test"""
    store = EventStoreManager()
    set_store(store)
    return store


@pytest.fixture
def client():
    return TestClient(app)


class TestHealthEndpoint:

    def test_health_check(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert "uptime_seconds" in data

    def test_metrics(self, client):
        resp = client.get("/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_events" in data
        assert "events_by_table" in data


class TestEventsEndpoint:

    def test_post_single_event(self, client):
        resp = client.post("/api/v1/events", json={
            "table": "ORDERS",
            "operation": "c",
            "data": {"ORDER_ID": 1001, "TOTAL_AMOUNT": 299.99},
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "accepted"
        assert data["event_id"] is not None

    def test_post_event_batch(self, client):
        resp = client.post("/api/v1/events/batch", json={
            "events": [
                {"table": "ORDERS", "operation": "c", "data": {"ORDER_ID": 1001}},
                {"table": "ORDERS", "operation": "u", "data": {"ORDER_ID": 1002}},
            ]
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["accepted"] == 2
        assert data["rejected"] == 0

    def test_get_events(self, client, fresh_store):
        fresh_store.events.add({"table": "ORDERS", "data": {"ORDER_ID": 1}})
        fresh_store.events.add({"table": "CUSTOMERS", "data": {"CUSTOMER_ID": 1}})

        resp = client.get("/api/v1/events")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 2

    def test_get_events_filter_by_table(self, client, fresh_store):
        fresh_store.events.add({"table": "ORDERS"})
        fresh_store.events.add({"table": "CUSTOMERS"})

        resp = client.get("/api/v1/events?table=ORDERS")
        data = resp.json()
        assert data["count"] == 1


class TestEnrichedOrdersEndpoint:

    def test_post_enriched_order(self, client):
        resp = client.post("/api/v1/enriched-orders", json={
            "order_id": 1001,
            "customer_id": 501,
            "customer_name": "John Doe",
            "customer_tier": "GOLD",
            "total_amount": 299.99,
        })
        assert resp.status_code == 200
        assert resp.json()["status"] == "accepted"

    def test_post_enriched_batch(self, client):
        resp = client.post("/api/v1/enriched-orders/batch", json={
            "orders": [
                {"order_id": 1001, "total_amount": 100.0},
                {"order_id": 1002, "total_amount": 200.0},
            ]
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["accepted"] == 2

    def test_get_enriched_orders(self, client, fresh_store):
        fresh_store.enriched_orders.add({"order_id": 1001})
        resp = client.get("/api/v1/enriched-orders")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1


class TestExtractedItemsEndpoint:

    def test_post_order_item(self, client):
        resp = client.post("/api/v1/order-items", json={
            "order_id": 1001,
            "product_id": 10,
            "quantity": 2,
            "unit_price": 49.99,
            "subtotal": 99.98,
        })
        assert resp.status_code == 200
        assert resp.json()["status"] == "accepted"

    def test_post_order_items_batch(self, client):
        resp = client.post("/api/v1/order-items/batch", json=[
            {"order_id": 1001, "product_id": 10, "quantity": 2},
            {"order_id": 1001, "product_id": 20, "quantity": 1},
        ])
        assert resp.status_code == 200
        data = resp.json()
        assert data["accepted"] == 2

    def test_get_order_items(self, client, fresh_store):
        fresh_store.extracted_items.add({"order_id": 1001, "product_id": 10})
        resp = client.get("/api/v1/order-items")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1


class TestNotificationsEndpoint:

    def test_post_notification(self, client):
        resp = client.post("/api/v1/notifications", json={
            "event_type": "HIGH_VALUE_ORDER",
            "message": "Order 1001 amount $10,000",
            "severity": "warning",
        })
        assert resp.status_code == 200
        assert resp.json()["status"] == "accepted"

    def test_get_notifications(self, client, fresh_store):
        fresh_store.notifications.add({
            "event_type": "HIGH_VALUE_ORDER",
            "severity": "warning",
            "message": "test",
        })

        resp = client.get("/api/v1/notifications")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1

    def test_get_notifications_by_severity(self, client, fresh_store):
        fresh_store.notifications.add({"severity": "warning", "event_type": "A"})
        fresh_store.notifications.add({"severity": "error", "event_type": "B"})

        resp = client.get("/api/v1/notifications?severity=error")
        data = resp.json()
        assert data["count"] == 1
