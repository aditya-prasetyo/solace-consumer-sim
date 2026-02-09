"""
CDC Event Processor for PySpark
Handles deduplication, ordering, and operation-specific processing
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class CDCProcessor:
    """Process CDC events: parse, deduplicate, apply operations"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def parse_events(
        self, table: str, raw_events: List[Dict[str, Any]]
    ) -> DataFrame:
        """
        Parse raw CDC events into a DataFrame.
        Each event contains: payload.before, payload.after, payload.op, payload.ts_ms, payload.source
        """
        parsed_rows = []
        for event in raw_events:
            payload = event if "op" in event else event.get("payload", {})
            op = payload.get("op", "c")
            ts_ms = payload.get("ts_ms", int(datetime.now().timestamp() * 1000))
            source = payload.get("source", {})

            # For insert/update -> use "after" data
            # For delete -> use "before" data
            if op in ("c", "r"):  # create or snapshot read
                data = payload.get("after", {})
            elif op == "u":  # update
                data = payload.get("after", {})
            elif op == "d":  # delete
                data = payload.get("before", {})
            else:
                continue

            if data:
                row = {
                    "_cdc_op": op,
                    "_cdc_ts_ms": ts_ms,
                    "_cdc_table": source.get("table", table),
                    "_cdc_scn": source.get("scn", ""),
                }
                row.update(data)
                parsed_rows.append(row)

        if not parsed_rows:
            return self.spark.createDataFrame([], schema="string")

        return self.spark.createDataFrame(parsed_rows)

    def deduplicate(
        self,
        df: DataFrame,
        key_column: str,
        order_column: str = "_cdc_ts_ms"
    ) -> DataFrame:
        """
        Deduplicate events: keep latest event per key.
        Uses timestamp ordering to resolve duplicates.
        """
        if df.rdd.isEmpty():
            return df

        window = Window.partitionBy(key_column).orderBy(F.col(order_column).desc())

        return (
            df.withColumn("_row_num", F.row_number().over(window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

    def apply_operations(
        self,
        df: DataFrame,
        op_column: str = "_cdc_op"
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Split DataFrame by CDC operation type.
        Returns: (inserts, updates, deletes)
        """
        if df.rdd.isEmpty():
            empty = self.spark.createDataFrame([], df.schema)
            return empty, empty, empty

        inserts = df.filter(F.col(op_column).isin("c", "r"))
        updates = df.filter(F.col(op_column) == "u")
        deletes = df.filter(F.col(op_column) == "d")

        return inserts, updates, deletes

    def add_processing_metadata(self, df: DataFrame) -> DataFrame:
        """Add processing metadata columns"""
        if df.rdd.isEmpty():
            return df

        return (
            df.withColumn("processed_at", F.current_timestamp())
            .withColumn("source_ts",
                F.from_unixtime(F.col("_cdc_ts_ms") / 1000).cast("timestamp")
            )
            .withColumn("operation", F.col("_cdc_op"))
        )

    def process_batch(
        self,
        table: str,
        raw_events: List[Dict[str, Any]],
        key_column: str
    ) -> Optional[DataFrame]:
        """
        Full batch processing pipeline:
        1. Parse events
        2. Deduplicate
        3. Add metadata
        """
        if not raw_events:
            return None

        # Parse
        df = self.parse_events(table, raw_events)
        if df.rdd.isEmpty():
            return None

        # Deduplicate
        df = self.deduplicate(df, key_column)

        # Add metadata
        df = self.add_processing_metadata(df)

        logger.debug(f"Processed batch for {table}: {df.count()} records")
        return df


# Table primary key mapping
TABLE_KEY_MAP = {
    "ORDERS": "ORDER_ID",
    "ORDER_ITEMS": "ITEM_ID",
    "CUSTOMERS": "CUSTOMER_ID",
    "PRODUCTS": "PRODUCT_ID",
    "AUDIT_LOG": "LOG_ID",
}
