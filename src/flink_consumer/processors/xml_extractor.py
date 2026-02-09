"""
XML Extractor for PyFlink CDC Consumer
FlatMap: 1 order with XML → N extracted item records
"""

import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def extract_shipping_xml(record: Dict[str, Any], xml_column: str = "SHIPPING_INFO") -> Dict[str, Any]:
    """
    Extract XML items from a single record.
    Returns dict with:
      - 'items': list of extracted item dicts
      - 'clean_record': record without XML column
      - 'error': error message if XML is malformed (for DLQ)
    """
    xml_data = record.get(xml_column)
    clean_record = {k: v for k, v in record.items() if k != xml_column}

    if not xml_data or not isinstance(xml_data, str) or not xml_data.strip():
        return {"items": [], "clean_record": clean_record, "error": None}

    try:
        root = ET.fromstring(xml_data)
        items_element = root.find("items")
        if items_element is None:
            return {"items": [], "clean_record": clean_record, "error": None}

        order_id = record.get("ORDER_ID")
        items = []

        for item_elem in items_element.findall("item"):
            product_id = _safe_int(item_elem.get("product_id"))
            quantity = _safe_int(item_elem.get("quantity"))
            unit_price = _safe_float(item_elem.get("unit_price"))
            subtotal = _safe_float(item_elem.get("subtotal"))

            if subtotal is None and quantity and unit_price:
                subtotal = round(quantity * unit_price, 2)

            items.append({
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "subtotal": subtotal,
                "extracted_from": "SHIPPING_INFO_XML",
                "source_ts": record.get("_source_ts"),
            })

        return {"items": items, "clean_record": clean_record, "error": None}

    except ET.ParseError as e:
        logger.warning(f"Malformed XML for ORDER_ID={record.get('ORDER_ID')}: {e}")
        return {
            "items": [],
            "clean_record": clean_record,
            "error": f"XML ParseError: {e}",
        }
    except Exception as e:
        logger.error(f"XML extraction error: {e}")
        return {
            "items": [],
            "clean_record": clean_record,
            "error": str(e),
        }


def extract_batch(
    records: List[Dict[str, Any]], xml_column: str = "SHIPPING_INFO"
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Extract XML from a batch of records.
    Returns: (clean_records, extracted_items, error_records_for_dlq)
    """
    clean_records = []
    all_items = []
    errors = []

    for record in records:
        result = extract_shipping_xml(record, xml_column)
        clean_records.append(result["clean_record"])

        if result["items"]:
            all_items.extend(result["items"])

        if result["error"]:
            errors.append({
                "record": record,
                "error": result["error"],
            })

    return clean_records, all_items, errors


def _safe_int(val: Optional[str]) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val: Optional[str]) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
