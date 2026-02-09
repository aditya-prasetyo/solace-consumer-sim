"""
Shared test fixtures for CDC Architecture POC
"""

import sys
import os
import pytest

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---- Sample CDC event fixtures ----

@pytest.fixture
def sample_order_event():
    """Sample Debezium CDC order event"""
    return {
        "payload": {
            "op": "c",
            "ts_ms": 1700000000000,
            "source": {"table": "ORDERS", "scn": "12345"},
            "after": {
                "ORDER_ID": 1001,
                "CUSTOMER_ID": 501,
                "ORDER_DATE": "2024-01-15",
                "TOTAL_AMOUNT": 299.99,
                "STATUS": "PENDING",
                "SHIPPING_INFO": '<shipping_info><items><item product_id="10" quantity="2" unit_price="49.99" subtotal="99.98"/><item product_id="20" quantity="1" unit_price="200.01" subtotal="200.01"/></items></shipping_info>',
            },
        }
    }


@pytest.fixture
def sample_customer_event():
    """Sample Debezium CDC customer event"""
    return {
        "payload": {
            "op": "c",
            "ts_ms": 1700000001000,
            "source": {"table": "CUSTOMERS"},
            "after": {
                "CUSTOMER_ID": 501,
                "FIRST_NAME": "John",
                "LAST_NAME": "Doe",
                "EMAIL": "john.doe@example.com",
                "TIER": "GOLD",
                "CITY": "Jakarta",
            },
        }
    }


@pytest.fixture
def sample_order_item_event():
    """Sample Debezium CDC order item event"""
    return {
        "payload": {
            "op": "c",
            "ts_ms": 1700000002000,
            "source": {"table": "ORDER_ITEMS"},
            "after": {
                "ITEM_ID": 5001,
                "ORDER_ID": 1001,
                "PRODUCT_ID": 10,
                "QUANTITY": 2,
                "UNIT_PRICE": 49.99,
                "SUBTOTAL": 99.98,
            },
        }
    }


@pytest.fixture
def sample_product_event():
    """Sample Debezium CDC product event"""
    return {
        "payload": {
            "op": "c",
            "ts_ms": 1700000003000,
            "source": {"table": "PRODUCTS"},
            "after": {
                "PRODUCT_ID": 10,
                "NAME": "Widget Pro",
                "CATEGORY": "Electronics",
                "PRICE": 49.99,
                "STOCK_QUANTITY": 100,
                "STATUS": "ACTIVE",
            },
        }
    }


@pytest.fixture
def sample_customer_data():
    """Customer data dict (parsed, for state store)"""
    return {
        "CUSTOMER_ID": 501,
        "FIRST_NAME": "John",
        "LAST_NAME": "Doe",
        "EMAIL": "john.doe@example.com",
        "TIER": "GOLD",
        "CITY": "Jakarta",
    }


@pytest.fixture
def sample_product_data():
    """Product data dict (parsed, for state store)"""
    return {
        "PRODUCT_ID": 10,
        "NAME": "Widget Pro",
        "CATEGORY": "Electronics",
        "PRICE": 49.99,
        "STOCK_QUANTITY": 100,
        "STATUS": "ACTIVE",
    }


@pytest.fixture
def sample_order_item_data():
    """Order item data dict"""
    return {
        "ITEM_ID": 5001,
        "ORDER_ID": 1001,
        "PRODUCT_ID": 10,
        "QUANTITY": 2,
        "UNIT_PRICE": 49.99,
        "SUBTOTAL": 99.98,
    }


@pytest.fixture
def sample_xml_valid():
    """Valid shipping XML"""
    return (
        '<shipping_info>'
        '<items>'
        '<item product_id="10" quantity="2" unit_price="49.99" subtotal="99.98"/>'
        '<item product_id="20" quantity="1" unit_price="200.01" subtotal="200.01"/>'
        '</items>'
        '<address>Jakarta</address>'
        '</shipping_info>'
    )


@pytest.fixture
def sample_xml_malformed():
    """Malformed XML"""
    return '<shipping_info><items><item product_id="10"'


@pytest.fixture
def sample_xml_no_items():
    """Valid XML but no items element"""
    return '<shipping_info><address>Jakarta</address></shipping_info>'
