"""
CDC Events Routes - REST API endpoints for receiving and querying CDC events
"""

import logging
import uuid
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from ..models.events import (
    CDCEventRequest, CDCBatchRequest,
    EnrichedOrderRequest, EnrichedBatchRequest,
    OrderItemExtractedRequest, NotificationRequest,
    EventResponse, BatchResponse,
)
from ..storage import EventStoreManager

logger = logging.getLogger(__name__)

router = APIRouter()

# Will be set by main app
store: Optional[EventStoreManager] = None


def set_store(store_manager: EventStoreManager) -> None:
    """Set the store manager (called during app startup)"""
    global store
    store = store_manager


# ---- CDC Events ----

@router.post("/api/v1/events", response_model=EventResponse, tags=["events"])
async def post_event(event: CDCEventRequest):
    """Receive a single CDC event"""
    event_id = store.events.add({
        "table": event.table,
        "operation": event.operation,
        "data": event.data,
        "source_ts": event.source_ts.isoformat() if event.source_ts else None,
        "consumer_id": event.consumer_id,
    })
    store.record_event(event.table, event.operation)
    logger.debug(f"Event received: {event.table}/{event.operation}")
    return EventResponse(status="accepted", event_id=event_id)


@router.post("/api/v1/events/batch", response_model=BatchResponse, tags=["events"])
async def post_event_batch(batch: CDCBatchRequest):
    """Receive a batch of CDC events"""
    batch_id = batch.batch_id or str(uuid.uuid4())
    accepted = 0
    rejected = 0

    for event in batch.events:
        try:
            store.events.add({
                "table": event.table,
                "operation": event.operation,
                "data": event.data,
                "source_ts": event.source_ts.isoformat() if event.source_ts else None,
                "consumer_id": event.consumer_id,
                "batch_id": batch_id,
            })
            store.record_event(event.table, event.operation)
            accepted += 1
        except Exception as e:
            logger.error(f"Failed to store event: {e}")
            store.record_error()
            rejected += 1

    return BatchResponse(
        status="accepted",
        accepted=accepted,
        rejected=rejected,
        batch_id=batch_id
    )


@router.get("/api/v1/events", tags=["events"])
async def get_events(
    table: Optional[str] = None,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0)
):
    """Query stored events"""
    if table:
        events = store.events.get_by_filter("table", table, limit)
    else:
        events = store.events.get_all(limit, offset)
    return {"events": events, "count": len(events)}


# ---- Enriched Orders ----

@router.post("/api/v1/enriched-orders", response_model=EventResponse, tags=["enriched"])
async def post_enriched_order(order: EnrichedOrderRequest):
    """Receive an enriched order (from 3-way join)"""
    event_id = store.enriched_orders.add(order.model_dump())
    store.record_event("ENRICHED_ORDER", "enriched")
    return EventResponse(status="accepted", event_id=event_id)


@router.post("/api/v1/enriched-orders/batch", response_model=BatchResponse, tags=["enriched"])
async def post_enriched_orders_batch(batch: EnrichedBatchRequest):
    """Receive a batch of enriched orders"""
    batch_id = batch.batch_id or str(uuid.uuid4())
    accepted = 0

    for order in batch.orders:
        store.enriched_orders.add({**order.model_dump(), "batch_id": batch_id})
        accepted += 1

    store.record_event("ENRICHED_ORDER", "enriched")
    return BatchResponse(status="accepted", accepted=accepted, rejected=0, batch_id=batch_id)


@router.get("/api/v1/enriched-orders", tags=["enriched"])
async def get_enriched_orders(
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0)
):
    """Query enriched orders"""
    orders = store.enriched_orders.get_all(limit, offset)
    return {"orders": orders, "count": len(orders)}


# ---- Extracted XML Items ----

@router.post("/api/v1/order-items", response_model=EventResponse, tags=["items"])
async def post_order_item(item: OrderItemExtractedRequest):
    """Receive an extracted XML item"""
    event_id = store.extracted_items.add(item.model_dump())
    store.record_event("ORDER_ITEM_EXTRACTED", "extracted")
    return EventResponse(status="accepted", event_id=event_id)


@router.post("/api/v1/order-items/batch", response_model=BatchResponse, tags=["items"])
async def post_order_items_batch(items: List[OrderItemExtractedRequest]):
    """Receive a batch of extracted items"""
    accepted = 0
    for item in items:
        store.extracted_items.add(item.model_dump())
        accepted += 1

    store.record_event("ORDER_ITEM_EXTRACTED", "extracted")
    return BatchResponse(status="accepted", accepted=accepted, rejected=0)


@router.get("/api/v1/order-items", tags=["items"])
async def get_order_items(
    order_id: Optional[int] = None,
    limit: int = Query(default=100, le=1000)
):
    """Query extracted items"""
    if order_id:
        items = store.extracted_items.get_by_filter("order_id", order_id, limit)
    else:
        items = store.extracted_items.get_all(limit)
    return {"items": items, "count": len(items)}


# ---- Notifications ----

@router.post("/api/v1/notifications", response_model=EventResponse, tags=["notifications"])
async def post_notification(notification: NotificationRequest):
    """Receive a notification"""
    event_id = store.notifications.add(notification.model_dump())
    store.record_event("NOTIFICATION", notification.event_type)
    logger.info(f"Notification [{notification.severity}]: {notification.message}")
    return EventResponse(status="accepted", event_id=event_id)


@router.get("/api/v1/notifications", tags=["notifications"])
async def get_notifications(
    severity: Optional[str] = None,
    limit: int = Query(default=100, le=1000)
):
    """Query notifications"""
    if severity:
        notifs = store.notifications.get_by_filter("severity", severity, limit)
    else:
        notifs = store.notifications.get_all(limit)
    return {"notifications": notifs, "count": len(notifs)}
