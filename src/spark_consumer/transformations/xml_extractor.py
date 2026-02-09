"""
XML Extractor for PySpark CDC Consumer
Extracts data from XML columns (e.g., SHIPPING_INFO) into separate rows/tables
"""

import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, StructType, StructField,
    StringType, IntegerType, DoubleType
)

logger = logging.getLogger(__name__)

# Schema for extracted XML items
XML_ITEM_SCHEMA = ArrayType(
    StructType([
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("subtotal", DoubleType(), True),
    ])
)


def parse_shipping_xml(xml_string: Optional[str]) -> Optional[List[Dict[str, Any]]]:
    """
    Parse SHIPPING_INFO XML and extract items.

    Expected XML format:
    <shipping_info>
      <items>
        <item product_id="123" quantity="2" unit_price="49.99" />
        ...
      </items>
      <address>...</address>
      <carrier>...</carrier>
    </shipping_info>
    """
    if not xml_string or not xml_string.strip():
        return None

    try:
        root = ET.fromstring(xml_string)
        items_element = root.find("items")
        if items_element is None:
            return None

        items = []
        for item_elem in items_element.findall("item"):
            item = {
                "product_id": _safe_int(item_elem.get("product_id")),
                "quantity": _safe_int(item_elem.get("quantity")),
                "unit_price": _safe_float(item_elem.get("unit_price")),
                "subtotal": _safe_float(item_elem.get("subtotal")),
            }

            # Calculate subtotal if not provided
            if item["subtotal"] is None and item["quantity"] and item["unit_price"]:
                item["subtotal"] = round(item["quantity"] * item["unit_price"], 2)

            items.append(item)

        return items if items else None

    except ET.ParseError as e:
        logger.warning(f"Malformed XML: {e}")
        raise  # Let caller handle for DLQ routing
    except Exception as e:
        logger.error(f"XML extraction error: {e}")
        raise


def _safe_int(val: Optional[str]) -> Optional[int]:
    """Safe conversion to int"""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val: Optional[str]) -> Optional[float]:
    """Safe conversion to float"""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


class XMLExtractor:
    """Extract data from XML columns in CDC events"""

    def __init__(self, spark: SparkSession, xml_column: str = "SHIPPING_INFO"):
        self.spark = spark
        self.xml_column = xml_column

        # Register UDF
        self._parse_udf = F.udf(parse_shipping_xml, XML_ITEM_SCHEMA)

    def extract(self, orders_df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Extract XML items from orders DataFrame.

        Returns:
            - orders_clean: Orders DataFrame without XML column
            - items_extracted: Extracted items with order_id
        """
        if orders_df.rdd.isEmpty():
            empty_items = self.spark.createDataFrame(
                [], self._items_schema()
            )
            return orders_df, empty_items

        # Check if XML column exists
        if self.xml_column not in orders_df.columns:
            empty_items = self.spark.createDataFrame(
                [], self._items_schema()
            )
            return orders_df, empty_items

        # Filter rows with XML data
        has_xml = orders_df.filter(
            F.col(self.xml_column).isNotNull() &
            (F.length(F.col(self.xml_column)) > 0)
        )

        no_xml = orders_df.filter(
            F.col(self.xml_column).isNull() |
            (F.length(F.col(self.xml_column)) == 0)
        )

        if has_xml.rdd.isEmpty():
            orders_clean = orders_df.drop(self.xml_column)
            empty_items = self.spark.createDataFrame(
                [], self._items_schema()
            )
            return orders_clean, empty_items

        # Apply XML parsing UDF
        parsed = has_xml.withColumn(
            "_xml_items", self._parse_udf(F.col(self.xml_column))
        )

        # Filter successful parses
        parsed_ok = parsed.filter(F.col("_xml_items").isNotNull())
        parsed_fail = parsed.filter(F.col("_xml_items").isNull())

        # Explode items into separate rows
        items_extracted = (
            parsed_ok
            .select(
                F.col("ORDER_ID").alias("order_id"),
                F.col("source_ts"),
                F.col("processed_at"),
                F.explode("_xml_items").alias("item")
            )
            .select(
                "order_id",
                F.col("item.product_id").alias("product_id"),
                F.col("item.quantity").alias("quantity"),
                F.col("item.unit_price").alias("unit_price"),
                F.col("item.subtotal").alias("subtotal"),
                F.lit("SHIPPING_INFO_XML").alias("extracted_from"),
                "source_ts",
                "processed_at"
            )
        )

        # Clean orders: combine no_xml + parsed (without XML column)
        orders_clean = no_xml.drop(self.xml_column).union(
            parsed_ok.drop(self.xml_column, "_xml_items")
        )

        # Add back failed parses (they'll be routed to DLQ separately)
        if not parsed_fail.rdd.isEmpty():
            orders_clean = orders_clean.union(
                parsed_fail.drop(self.xml_column, "_xml_items")
            )

        return orders_clean, items_extracted

    def extract_from_records(
        self, records: List[Dict[str, Any]]
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Extract XML items from raw records (non-Spark path).
        Returns: (cleaned_records, extracted_items)
        """
        cleaned = []
        extracted = []

        for record in records:
            xml_data = record.get(self.xml_column)
            clean_record = {k: v for k, v in record.items() if k != self.xml_column}

            if xml_data:
                try:
                    items = parse_shipping_xml(xml_data)
                    if items:
                        order_id = record.get("ORDER_ID")
                        for item in items:
                            item["order_id"] = order_id
                            item["extracted_from"] = "SHIPPING_INFO_XML"
                            extracted.append(item)
                except Exception:
                    # XML parse failure - record goes to DLQ
                    clean_record["_xml_error"] = True

            cleaned.append(clean_record)

        return cleaned, extracted

    def _items_schema(self) -> StructType:
        """Schema for extracted items DataFrame"""
        return StructType([
            StructField("order_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("subtotal", DoubleType(), True),
            StructField("extracted_from", StringType(), True),
            StructField("source_ts", StringType(), True),
            StructField("processed_at", StringType(), True),
        ])
