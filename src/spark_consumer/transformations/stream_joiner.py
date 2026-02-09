"""
3-Way Stream Joiner for PySpark CDC Consumer
Joins ORDERS + CUSTOMERS + ORDER_ITEMS (+ PRODUCTS) for enrichment
"""

import logging
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ..config import JoinConfig

logger = logging.getLogger(__name__)


class StreamJoiner:
    """Performs 3-way join across CDC streams for order enrichment"""

    def __init__(self, spark: SparkSession, config: JoinConfig):
        self.spark = spark
        self.config = config

    def enrich_orders(
        self,
        orders_df: DataFrame,
        customers_df: DataFrame,
        order_items_df: DataFrame,
        products_df: Optional[DataFrame] = None
    ) -> Optional[DataFrame]:
        """
        3-way join: ORDERS + CUSTOMERS + ORDER_ITEMS
        Optional 4th: + PRODUCTS

        Result -> cdc_orders_enriched table
        """
        if orders_df.rdd.isEmpty():
            return None

        # Prepare customers: get latest per customer_id
        if not customers_df.rdd.isEmpty():
            customers_latest = self._get_latest(
                customers_df, "CUSTOMER_ID", "_cdc_ts_ms"
            )
            # Select relevant customer columns
            customers_select = customers_latest.select(
                F.col("CUSTOMER_ID").alias("_cust_id"),
                F.concat_ws(" ", F.col("FIRST_NAME"), F.col("LAST_NAME")).alias("customer_name"),
                F.col("TIER").alias("customer_tier"),
                F.col("CITY").alias("customer_region")
            )
        else:
            customers_select = None

        # Prepare order items: aggregate by order_id
        if not order_items_df.rdd.isEmpty():
            items_agg = (
                order_items_df
                .groupBy("ORDER_ID")
                .agg(
                    F.count("*").alias("item_count"),
                    F.sum("SUBTOTAL").alias("items_total")
                )
                .withColumnRenamed("ORDER_ID", "_item_order_id")
            )
        else:
            items_agg = None

        # Start with orders
        enriched = orders_df.select(
            F.col("ORDER_ID").alias("order_id"),
            F.col("CUSTOMER_ID").alias("customer_id"),
            F.col("TOTAL_AMOUNT").alias("total_amount"),
            F.col("ORDER_DATE").alias("order_date"),
            F.col("STATUS").alias("status"),
            F.col("source_ts"),
            F.col("processed_at"),
        )

        # Join 1: Orders + Customers (left join)
        if customers_select is not None:
            enriched = enriched.join(
                customers_select,
                enriched.customer_id == customers_select._cust_id,
                "left"
            ).drop("_cust_id")
        else:
            enriched = (
                enriched
                .withColumn("customer_name", F.lit(None).cast("string"))
                .withColumn("customer_tier", F.lit(None).cast("string"))
                .withColumn("customer_region", F.lit(None).cast("string"))
            )

        # Join 2: + Order Items (left join for item count)
        if items_agg is not None:
            enriched = enriched.join(
                items_agg,
                enriched.order_id == items_agg._item_order_id,
                "left"
            ).drop("_item_order_id")
        else:
            enriched = (
                enriched
                .withColumn("item_count", F.lit(0))
                .withColumn("items_total", F.lit(None).cast("double"))
            )

        # Add window timestamps
        enriched = enriched.withColumn(
            "window_start", F.current_timestamp()
        ).withColumn(
            "window_end", F.current_timestamp()
        )

        # Final column selection matching cdc_orders_enriched table
        result = enriched.select(
            "order_id",
            "customer_id",
            "customer_name",
            "customer_tier",
            "customer_region",
            "total_amount",
            "item_count",
            "order_date",
            "status",
            "window_start",
            "window_end",
            "processed_at"
        )

        logger.info(f"Enriched {result.count()} orders")
        return result

    def aggregate_orders(
        self,
        enriched_df: DataFrame
    ) -> Optional[DataFrame]:
        """
        Create window aggregations from enriched orders.
        Output -> cdc_order_aggregations table
        """
        if enriched_df is None or enriched_df.rdd.isEmpty():
            return None

        aggregated = (
            enriched_df
            .groupBy("customer_tier")
            .agg(
                F.count("*").alias("order_count"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("total_amount").alias("avg_order_value"),
                F.min("processed_at").alias("window_start"),
                F.max("processed_at").alias("window_end"),
            )
            .withColumn("product_category", F.lit(None).cast("string"))
            .withColumn("processed_at", F.current_timestamp())
        )

        # Final column selection matching cdc_order_aggregations table
        result = aggregated.select(
            "window_start",
            "window_end",
            "customer_tier",
            "product_category",
            "order_count",
            "total_revenue",
            "avg_order_value",
            "processed_at"
        )

        return result

    def _get_latest(
        self, df: DataFrame, key_col: str, ts_col: str
    ) -> DataFrame:
        """Get latest record per key"""
        window = Window.partitionBy(key_col).orderBy(F.col(ts_col).desc())
        return (
            df.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
