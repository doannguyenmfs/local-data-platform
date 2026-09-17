"""Merge PostgreSQL staging sales into an ACID Iceberg table."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import functions as F

from iceberg_common import iceberg_spark
from sales_batch import read_staging_table, require_environment, transform_sales, validate_sales


TABLE_NAME = "local.ecommerce.fact_sales"


def create_table(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.ecommerce")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            order_item_id STRING NOT NULL,
            order_id STRING NOT NULL,
            customer_id STRING NOT NULL,
            product_id STRING NOT NULL,
            order_date TIMESTAMP NOT NULL,
            sales_date DATE NOT NULL,
            order_status STRING,
            quantity INT,
            unit_price DECIMAL(12, 2),
            sales_amount DECIMAL(14, 2),
            payment_status STRING,
            payment_attempt_count BIGINT,
            source_loaded_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(order_date))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.target-file-size-bytes' = '134217728',
            'write.distribution-mode' = 'hash'
        )
        """
    )


def merge_sales(spark, updates) -> None:
    updates.createOrReplaceTempView("sales_updates")
    spark.sql(
        f"""
        MERGE INTO {TABLE_NAME} AS target
        USING sales_updates AS source
          ON target.order_item_id = source.order_item_id
        WHEN MATCHED
          AND source.source_loaded_at >= target.source_loaded_at
          THEN UPDATE SET *
        WHEN NOT MATCHED
          THEN INSERT *
        """
    )


def validate_table(spark) -> dict[str, Any]:
    metrics = spark.sql(
        f"""
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT order_item_id) AS distinct_order_item_count
        FROM {TABLE_NAME}
        """
    ).first()
    if metrics.row_count != metrics.distinct_order_item_count:
        raise ValueError("Iceberg fact_sales violates order_item_id grain")

    snapshot = spark.sql(
        f"""
        SELECT snapshot_id, committed_at, operation
        FROM {TABLE_NAME}.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
        """
    ).first()
    return {
        "row_count": int(metrics.row_count),
        "distinct_order_item_count": int(metrics.distinct_order_item_count),
        "snapshot_id": int(snapshot.snapshot_id) if snapshot else None,
        "operation": snapshot.operation if snapshot else None,
    }


def main() -> None:
    config = require_environment()
    spark = iceberg_spark("ecommerce-iceberg-sales")
    spark.sparkContext.setLogLevel("WARN")
    try:
        create_table(spark)
        current_high_watermark = (
            spark.table(TABLE_NAME).agg(F.max("source_loaded_at")).first()[0]
            or datetime(1900, 1, 1, tzinfo=timezone.utc)
        )
        updates = transform_sales(
            read_staging_table(spark, config, "orders"),
            read_staging_table(spark, config, "order_items"),
            read_staging_table(spark, config, "payments"),
        ).filter(F.col("source_loaded_at") > F.lit(current_high_watermark)).persist()
        source_metrics = validate_sales(updates)
        merge_sales(spark, updates)
        table_metrics = validate_table(spark)
        print(
            "ICEBERG_JOB_METRICS="
            + json.dumps({"source": source_metrics, "table": table_metrics})
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
