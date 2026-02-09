"""
Dynamic Schema Handler for PySpark CDC Consumer
Handles schema evolution: new columns auto-detected, removed columns treated as NULL
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType,
    BooleanType, TimestampType, MapType, StructType, StructField
)

logger = logging.getLogger(__name__)

# Known schema definitions (baseline)
KNOWN_SCHEMAS: Dict[str, List[Tuple[str, str]]] = {
    "ORDERS": [
        ("ORDER_ID", "int"),
        ("CUSTOMER_ID", "int"),
        ("ORDER_DATE", "timestamp"),
        ("TOTAL_AMOUNT", "double"),
        ("STATUS", "string"),
        ("SHIPPING_INFO", "string"),
        ("CREATED_AT", "timestamp"),
        ("UPDATED_AT", "timestamp"),
    ],
    "ORDER_ITEMS": [
        ("ITEM_ID", "int"),
        ("ORDER_ID", "int"),
        ("PRODUCT_ID", "int"),
        ("QUANTITY", "int"),
        ("UNIT_PRICE", "double"),
        ("SUBTOTAL", "double"),
        ("CREATED_AT", "timestamp"),
        ("UPDATED_AT", "timestamp"),
    ],
    "CUSTOMERS": [
        ("CUSTOMER_ID", "int"),
        ("FIRST_NAME", "string"),
        ("LAST_NAME", "string"),
        ("EMAIL", "string"),
        ("PHONE", "string"),
        ("ADDRESS", "string"),
        ("CITY", "string"),
        ("COUNTRY", "string"),
        ("TIER", "string"),
        ("CREATED_AT", "timestamp"),
        ("UPDATED_AT", "timestamp"),
    ],
    "PRODUCTS": [
        ("PRODUCT_ID", "int"),
        ("NAME", "string"),
        ("DESCRIPTION", "string"),
        ("CATEGORY", "string"),
        ("PRICE", "double"),
        ("STOCK_QUANTITY", "int"),
        ("STATUS", "string"),
        ("CREATED_AT", "timestamp"),
        ("UPDATED_AT", "timestamp"),
    ],
    "AUDIT_LOG": [
        ("LOG_ID", "int"),
        ("ACTION", "string"),
        ("ENTITY_TYPE", "string"),
        ("ENTITY_ID", "int"),
        ("USER_ID", "int"),
        ("DETAILS", "string"),
        ("IP_ADDRESS", "string"),
        ("CREATED_AT", "timestamp"),
    ],
}


def _spark_type(type_name: str):
    """Map type name to PySpark type"""
    mapping = {
        "int": IntegerType(),
        "long": LongType(),
        "double": DoubleType(),
        "string": StringType(),
        "boolean": BooleanType(),
        "timestamp": TimestampType(),
    }
    return mapping.get(type_name, StringType())


class SchemaHandler:
    """Handles dynamic schema evolution for CDC events"""

    def __init__(self):
        self._seen_columns: Dict[str, Set[str]] = {}
        self._column_types: Dict[str, Dict[str, str]] = {}

        # Initialize with known schemas
        for table, columns in KNOWN_SCHEMAS.items():
            self._seen_columns[table] = {col for col, _ in columns}
            self._column_types[table] = {col: dtype for col, dtype in columns}

    def get_known_columns(self, table: str) -> Set[str]:
        """Get known columns for a table"""
        return self._seen_columns.get(table, set())

    def detect_new_columns(self, table: str, data: Dict[str, Any]) -> List[str]:
        """Detect columns not in the known schema"""
        known = self.get_known_columns(table)
        if not known:
            return list(data.keys())
        return [col for col in data.keys() if col not in known]

    def register_columns(self, table: str, data: Dict[str, Any]) -> List[str]:
        """Register new columns and return list of newly discovered ones"""
        if table not in self._seen_columns:
            self._seen_columns[table] = set()
            self._column_types[table] = {}

        new_columns = []
        for col, val in data.items():
            if col not in self._seen_columns[table]:
                self._seen_columns[table].add(col)
                inferred_type = self._infer_type(val)
                self._column_types[table][col] = inferred_type
                new_columns.append(col)
                logger.info(
                    f"Schema evolution: new column '{col}' ({inferred_type}) "
                    f"detected in table '{table}'"
                )

        return new_columns

    def _infer_type(self, value: Any) -> str:
        """Infer PySpark-compatible type from Python value"""
        if value is None:
            return "string"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "long" if abs(value) > 2_147_483_647 else "int"
        elif isinstance(value, float):
            return "double"
        elif isinstance(value, dict):
            return "string"  # Store as JSON string
        elif isinstance(value, list):
            return "string"  # Store as JSON string
        return "string"

    def normalize_record(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize record against known schema:
        - Known columns missing from data -> set to None
        - Extra columns -> include them (schema evolution)
        """
        # Register any new columns
        self.register_columns(table, data)

        # Start with all known columns set to None
        known = self.get_known_columns(table)
        normalized = {col: None for col in known}

        # Fill with actual data
        for col, val in data.items():
            if isinstance(val, (dict, list)):
                normalized[col] = json.dumps(val)
            else:
                normalized[col] = val

        return normalized

    def build_struct_type(self, table: str) -> StructType:
        """Build PySpark StructType for table"""
        fields = []
        for col in sorted(self._seen_columns.get(table, set())):
            col_type = self._column_types.get(table, {}).get(col, "string")
            fields.append(StructField(col, _spark_type(col_type), nullable=True))
        return StructType(fields)

    def apply_schema_to_df(self, df: DataFrame, table: str) -> DataFrame:
        """
        Ensure DataFrame has all known columns.
        Missing columns -> NULL, extra columns -> preserved.
        """
        known = self.get_known_columns(table)

        # Add missing columns as NULL
        for col in known:
            if col not in df.columns:
                col_type = self._column_types.get(table, {}).get(col, "string")
                df = df.withColumn(col, F.lit(None).cast(_spark_type(col_type)))

        return df

    def prepare_dynamic_record(
        self, table: str, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Prepare record for dynamic schema table (JSONB storage).
        Splits into known columns + dynamic JSONB data column.
        """
        known_schema = KNOWN_SCHEMAS.get(table, [])
        known_cols = {col for col, _ in known_schema}

        # Separate known vs extra fields
        known_data = {}
        extra_data = {}

        for col, val in data.items():
            if col in known_cols:
                known_data[col] = val
            else:
                extra_data[col] = val

        return {
            "known": known_data,
            "extra": extra_data,
            "has_extra": len(extra_data) > 0
        }
