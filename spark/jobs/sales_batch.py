"""Build an order-item sales dataset from PostgreSQL staging with PySpark.

This is the plain-Parquet teaching job.  It isolates read, pure transformation,
validation and write so the business logic can be unit-tested without external
services.  ``iceberg_sales.py`` reuses the same functions but replaces the
non-transactional overwrite sink with an Iceberg table.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


REQUIRED_ENV_VARS = (
    "ECOMMERCE_POSTGRES_HOST",
    "ECOMMERCE_POSTGRES_PORT",
    "ECOMMERCE_POSTGRES_USER",
    "ECOMMERCE_POSTGRES_PASSWORD",
    "ECOMMERCE_POSTGRES_DB",
)


def require_environment(names: Iterable[str] = REQUIRED_ENV_VARS) -> dict[str, str]:
    """Return required configuration without ever logging secret values."""
    missing = [name for name in names if not os.getenv(name)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    return {name: os.environ[name] for name in names}


def read_staging_table(
    spark: SparkSession,
    config: dict[str, str],
    table_name: str,
) -> DataFrame:
    """Read one current-state staging table through PostgreSQL JDBC.

    The UUID business keys are poor range-partition columns, so this lab avoids
    fake JDBC parallelism that would produce overlapping/full-table scans.
    ``fetchsize`` still reduces network round trips.
    """
    jdbc_url = (
        f"jdbc:postgresql://{config['ECOMMERCE_POSTGRES_HOST']}:"
        f"{config['ECOMMERCE_POSTGRES_PORT']}/{config['ECOMMERCE_POSTGRES_DB']}"
    )
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"staging.{table_name}")
        .option("user", config["ECOMMERCE_POSTGRES_USER"])
        .option("password", config["ECOMMERCE_POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )


def transform_sales(
    orders: DataFrame,
    order_items: DataFrame,
    payments: DataFrame,
) -> DataFrame:
    """Return one enriched row per order item without performing external I/O."""
    # Payments are one-to-many with orders.  Aggregate before the item join or
    # every payment attempt would multiply every order item and break the grain.
    payment_metrics = payments.groupBy("order_id").agg(
        F.count("payment_id").alias("payment_attempt_count"),
        F.sum(
            F.when(F.col("payment_status") == "completed", F.lit(1)).otherwise(
                F.lit(0)
            )
        ).alias("completed_payment_count"),
        F.max("loaded_at").alias("payment_loaded_at"),
    )

    payment_metrics = payment_metrics.withColumn(
        "payment_status",
        F.when(
            F.col("completed_payment_count") == F.col("payment_attempt_count"),
            F.lit("paid"),
        )
        .when(F.col("completed_payment_count") > 0, F.lit("partially_paid"))
        .otherwise(F.lit("unpaid")),
    )

    # The inner header/item join preserves valid item grain.  Payment remains a
    # left join because an unpaid/new order may have no payment attempts yet.
    sales = (
        orders.alias("orders")
        .join(
            order_items.alias("items"),
            F.col("orders.order_id") == F.col("items.order_id"),
            "inner",
        )
        .join(
            payment_metrics.alias("payments"),
            F.col("orders.order_id") == F.col("payments.order_id"),
            "left",
        )
        .select(
            F.col("items.order_item_id").cast("string").alias("order_item_id"),
            F.col("orders.order_id").cast("string").alias("order_id"),
            F.col("orders.customer_id").cast("string").alias("customer_id"),
            F.col("items.product_id").cast("string").alias("product_id"),
            F.col("orders.order_date"),
            F.to_date(F.col("orders.order_date")).alias("sales_date"),
            F.col("orders.status").alias("order_status"),
            F.col("items.quantity"),
            F.col("items.unit_price"),
            (F.col("items.quantity") * F.col("items.unit_price"))
            .cast("decimal(14,2)")
            .alias("sales_amount"),
            F.coalesce(F.col("payments.payment_status"), F.lit("unpaid")).alias(
                "payment_status"
            ),
            F.coalesce(F.col("payments.payment_attempt_count"), F.lit(0)).alias(
                "payment_attempt_count"
            ),
            # Downstream incremental work must react when *any* contributing
            # staging relation changes, hence the greatest load timestamp.
            F.greatest(
                F.col("orders.loaded_at"),
                F.col("items.loaded_at"),
                F.coalesce(
                    F.col("payments.payment_loaded_at"),
                    F.to_timestamp(F.lit("1900-01-01 00:00:00")),
                ),
            ).alias("source_loaded_at"),
        )
    )
    return sales


def validate_sales(sales: DataFrame) -> dict[str, int]:
    """Fail the job before writing when grain or core measures are invalid."""
    # One aggregate action computes all control totals in a single pass.
    metrics = sales.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("order_item_id").alias("distinct_order_item_count"),
        F.sum(
            F.when(
                F.col("order_item_id").isNull()
                | F.col("order_id").isNull()
                | F.col("sales_date").isNull(),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("null_key_count"),
        F.sum(
            F.when(
                (F.col("quantity") < 0) | (F.col("sales_amount") < 0), F.lit(1)
            ).otherwise(F.lit(0))
        ).alias("invalid_measure_count"),
    ).first()

    result = {name: int(metrics[name] or 0) for name in metrics.asDict()}
    errors: list[str] = []
    if result["row_count"] != result["distinct_order_item_count"]:
        errors.append("order_item_id does not preserve the declared grain")
    if result["null_key_count"]:
        errors.append(f"{result['null_key_count']} rows contain null business keys")
    if result["invalid_measure_count"]:
        errors.append(f"{result['invalid_measure_count']} rows have invalid measures")
    if errors:
        raise ValueError("Spark sales validation failed: " + "; ".join(errors))
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default=os.getenv("SPARK_SALES_OUTPUT", "/opt/lakehouse/parquet/sales"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = require_environment()
    spark = (
        SparkSession.builder.appName("ecommerce-sales-batch")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Validation and write are separate actions.  Persist prevents Spark
        # from rereading JDBC and recomputing the joins for the second action.
        sales = transform_sales(
            read_staging_table(spark, config, "orders"),
            read_staging_table(spark, config, "order_items"),
            read_staging_table(spark, config, "payments"),
        ).persist()
        metrics = validate_sales(sales)
        (
            # Repartition controls execution distribution; partitionBy controls
            # the physical directory layout.  Date has bounded, query-friendly
            # cardinality unlike UUID identifiers.
            sales.repartition("sales_date")
            .write.mode("overwrite")
            .partitionBy("sales_date")
            .parquet(args.output)
        )
        print("SPARK_JOB_METRICS=" + json.dumps({**metrics, "output": args.output}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
