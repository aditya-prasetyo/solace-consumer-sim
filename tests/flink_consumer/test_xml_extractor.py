"""
Tests for PyFlink XML Extractor
Covers: extract_shipping_xml, extract_batch, malformed XML, DLQ errors
"""

import pytest

from src.flink_consumer.processors.xml_extractor import (
    extract_shipping_xml,
    extract_batch,
    _safe_int,
    _safe_float,
)


class TestExtractShippingXML:

    def test_valid_xml(self, sample_xml_valid):
        record = {"ORDER_ID": 1001, "SHIPPING_INFO": sample_xml_valid}
        result = extract_shipping_xml(record)

        assert result["error"] is None
        assert len(result["items"]) == 2
        assert result["items"][0]["order_id"] == 1001
        assert result["items"][0]["product_id"] == 10
        assert result["items"][0]["quantity"] == 2
        assert "SHIPPING_INFO" not in result["clean_record"]
        assert result["clean_record"]["ORDER_ID"] == 1001

    def test_malformed_xml(self, sample_xml_malformed):
        record = {"ORDER_ID": 1001, "SHIPPING_INFO": sample_xml_malformed}
        result = extract_shipping_xml(record)

        assert result["error"] is not None
        assert "ParseError" in result["error"]
        assert result["items"] == []

    def test_no_xml_column(self):
        record = {"ORDER_ID": 1001, "TOTAL_AMOUNT": 100}
        result = extract_shipping_xml(record)

        assert result["error"] is None
        assert result["items"] == []

    def test_empty_xml(self):
        record = {"ORDER_ID": 1001, "SHIPPING_INFO": ""}
        result = extract_shipping_xml(record)

        assert result["items"] == []
        assert result["error"] is None

    def test_xml_no_items_element(self, sample_xml_no_items):
        record = {"ORDER_ID": 1001, "SHIPPING_INFO": sample_xml_no_items}
        result = extract_shipping_xml(record)

        assert result["items"] == []
        assert result["error"] is None

    def test_subtotal_auto_calc(self):
        xml = (
            '<shipping_info><items>'
            '<item product_id="5" quantity="4" unit_price="25.00"/>'
            '</items></shipping_info>'
        )
        record = {"ORDER_ID": 1001, "SHIPPING_INFO": xml}
        result = extract_shipping_xml(record)

        assert result["items"][0]["subtotal"] == 100.00

    def test_extracted_from_field(self, sample_xml_valid):
        record = {"ORDER_ID": 1001, "SHIPPING_INFO": sample_xml_valid}
        result = extract_shipping_xml(record)

        assert result["items"][0]["extracted_from"] == "SHIPPING_INFO_XML"

    def test_custom_xml_column(self):
        xml = '<shipping_info><items><item product_id="1" quantity="1" unit_price="10.00" subtotal="10.00"/></items></shipping_info>'
        record = {"ORDER_ID": 1001, "CUSTOM_XML": xml}
        result = extract_shipping_xml(record, xml_column="CUSTOM_XML")

        assert len(result["items"]) == 1


class TestExtractBatch:

    def test_batch_mixed(self, sample_xml_valid, sample_xml_malformed):
        records = [
            {"ORDER_ID": 1001, "SHIPPING_INFO": sample_xml_valid},
            {"ORDER_ID": 1002},  # No XML
            {"ORDER_ID": 1003, "SHIPPING_INFO": sample_xml_malformed},
        ]

        clean, items, errors = extract_batch(records)

        assert len(clean) == 3
        assert len(items) == 2  # From order 1001
        assert len(errors) == 1  # Order 1003
        assert errors[0]["record"]["ORDER_ID"] == 1003

    def test_batch_empty(self):
        clean, items, errors = extract_batch([])
        assert clean == []
        assert items == []
        assert errors == []

    def test_batch_all_valid(self, sample_xml_valid):
        records = [
            {"ORDER_ID": i, "SHIPPING_INFO": sample_xml_valid}
            for i in range(3)
        ]

        clean, items, errors = extract_batch(records)
        assert len(items) == 6  # 2 items per order * 3 orders
        assert len(errors) == 0


class TestFlinkSafeConversions:

    def test_safe_int(self):
        assert _safe_int("42") == 42
        assert _safe_int(None) is None
        assert _safe_int("abc") is None

    def test_safe_float(self):
        assert _safe_float("3.14") == 3.14
        assert _safe_float(None) is None
        assert _safe_float("xyz") is None
