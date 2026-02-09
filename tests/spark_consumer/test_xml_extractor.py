"""
Tests for PySpark XML Extractor
Covers: valid XML parsing, malformed XML, no items, subtotal calc, extract_from_records
"""

import pytest

from src.spark_consumer.transformations.xml_extractor import (
    parse_shipping_xml,
    _safe_int,
    _safe_float,
)


class TestParseShippingXML:
    """Tests for XML parsing function"""

    def test_valid_xml(self, sample_xml_valid):
        items = parse_shipping_xml(sample_xml_valid)
        assert items is not None
        assert len(items) == 2
        assert items[0]["product_id"] == 10
        assert items[0]["quantity"] == 2
        assert items[0]["unit_price"] == 49.99
        assert items[0]["subtotal"] == 99.98
        assert items[1]["product_id"] == 20

    def test_malformed_xml_raises(self, sample_xml_malformed):
        import xml.etree.ElementTree as ET
        with pytest.raises(ET.ParseError):
            parse_shipping_xml(sample_xml_malformed)

    def test_no_items_element(self, sample_xml_no_items):
        result = parse_shipping_xml(sample_xml_no_items)
        assert result is None

    def test_empty_string(self):
        assert parse_shipping_xml("") is None
        assert parse_shipping_xml(None) is None
        assert parse_shipping_xml("   ") is None

    def test_subtotal_auto_calculated(self):
        xml = (
            '<shipping_info><items>'
            '<item product_id="10" quantity="3" unit_price="10.00"/>'
            '</items></shipping_info>'
        )
        items = parse_shipping_xml(xml)
        assert items[0]["subtotal"] == 30.00

    def test_empty_items_element(self):
        xml = '<shipping_info><items></items></shipping_info>'
        result = parse_shipping_xml(xml)
        assert result is None


class TestSafeConversions:

    def test_safe_int_valid(self):
        assert _safe_int("42") == 42
        assert _safe_int("0") == 0

    def test_safe_int_invalid(self):
        assert _safe_int(None) is None
        assert _safe_int("abc") is None

    def test_safe_float_valid(self):
        assert _safe_float("3.14") == 3.14
        assert _safe_float("0") == 0.0

    def test_safe_float_invalid(self):
        assert _safe_float(None) is None
        assert _safe_float("xyz") is None


class TestXMLExtractorRecords:
    """Tests for extract_from_records (non-Spark path)"""

    def test_extract_from_records_with_xml(self):
        from src.spark_consumer.transformations.xml_extractor import XMLExtractor
        # Use non-Spark path
        extractor = XMLExtractor.__new__(XMLExtractor)
        extractor.xml_column = "SHIPPING_INFO"

        records = [
            {
                "ORDER_ID": 1001,
                "TOTAL_AMOUNT": 299.99,
                "SHIPPING_INFO": (
                    '<shipping_info><items>'
                    '<item product_id="10" quantity="2" unit_price="49.99" subtotal="99.98"/>'
                    '</items></shipping_info>'
                ),
            }
        ]

        cleaned, extracted = extractor.extract_from_records(records)
        assert len(cleaned) == 1
        assert "SHIPPING_INFO" not in cleaned[0]
        assert len(extracted) == 1
        assert extracted[0]["order_id"] == 1001
        assert extracted[0]["product_id"] == 10
        assert extracted[0]["extracted_from"] == "SHIPPING_INFO_XML"

    def test_extract_from_records_no_xml(self):
        from src.spark_consumer.transformations.xml_extractor import XMLExtractor
        extractor = XMLExtractor.__new__(XMLExtractor)
        extractor.xml_column = "SHIPPING_INFO"

        records = [
            {"ORDER_ID": 1001, "TOTAL_AMOUNT": 100.0},
        ]

        cleaned, extracted = extractor.extract_from_records(records)
        assert len(cleaned) == 1
        assert len(extracted) == 0

    def test_extract_from_records_malformed_xml(self):
        from src.spark_consumer.transformations.xml_extractor import XMLExtractor
        extractor = XMLExtractor.__new__(XMLExtractor)
        extractor.xml_column = "SHIPPING_INFO"

        records = [
            {"ORDER_ID": 1001, "SHIPPING_INFO": "<broken xml"},
        ]

        cleaned, extracted = extractor.extract_from_records(records)
        assert len(cleaned) == 1
        assert cleaned[0].get("_xml_error") is True
        assert len(extracted) == 0
