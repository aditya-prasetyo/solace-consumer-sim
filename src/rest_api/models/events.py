"""
Pydantic Models for CDC Events REST API
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---- Request Models ----

class CDCEventRequest(BaseModel):
    """Incoming CDC event from consumer"""
    table: str
    operation: str  # c, u, d
    data: Dict[str, Any]
    source_ts: Optional[datetime] = None
    consumer_id: Optional[str] = None


class CDCBatchRequest(BaseModel):
    """Batch of CDC events"""
    events: List[CDCEventRequest]
    batch_id: Optional[str] = None


class EnrichedOrderRequest(BaseModel):
    """Enriched order from 3-way join"""
    order_id: int
    customer_id: Optional[int] = None
    customer_name: Optional[str] = None
    customer_tier: Optional[str] = None
    customer_region: Optional[str] = None
    total_amount: Optional[float] = None
    item_count: Optional[int] = None
    order_date: Optional[datetime] = None
    status: Optional[str] = None
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None


class EnrichedBatchRequest(BaseModel):
    """Batch of enriched orders"""
    orders: List[EnrichedOrderRequest]
    batch_id: Optional[str] = None


class OrderItemExtractedRequest(BaseModel):
    """Extracted XML item"""
    order_id: int
    product_id: Optional[int] = None
    quantity: Optional[int] = None
    unit_price: Optional[float] = None
    subtotal: Optional[float] = None
    extracted_from: Optional[str] = "SHIPPING_INFO_XML"


class NotificationRequest(BaseModel):
    """Notification/alert event"""
    event_type: str
    message: str
    severity: str = "info"  # info, warning, error, critical
    payload: Optional[Dict[str, Any]] = None


# ---- Response Models ----

class EventResponse(BaseModel):
    """Response for event submission"""
    status: str = "accepted"
    event_id: Optional[str] = None
    message: Optional[str] = None


class BatchResponse(BaseModel):
    """Response for batch submission"""
    status: str = "accepted"
    accepted: int = 0
    rejected: int = 0
    batch_id: Optional[str] = None


class HealthResponse(BaseModel):
    """Health check response"""
    status: str = "healthy"
    uptime_seconds: float = 0
    events_received: int = 0
    events_stored: int = 0


class MetricsResponse(BaseModel):
    """Metrics response"""
    total_events: int = 0
    events_by_table: Dict[str, int] = {}
    events_by_operation: Dict[str, int] = {}
    enriched_orders: int = 0
    extracted_items: int = 0
    notifications: int = 0
    errors: int = 0
    uptime_seconds: float = 0
