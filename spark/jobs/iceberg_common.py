"""Build the one canonical Spark/Iceberg session used by every lakehouse job.

Centralizing this configuration prevents batch, streaming, maintenance and
validation jobs from silently pointing at different catalogs, warehouses or
time zones.  PostgreSQL is the durable catalog backend; MinIO is the object
store containing Iceberg metadata/manifests/data files.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def iceberg_spark(app_name: str) -> SparkSession:
    """Create a SparkSession connected to the durable JDBC Iceberg catalog."""
    required = (
        "ECOMMERCE_POSTGRES_HOST",
        "ECOMMERCE_POSTGRES_PORT",
        "ECOMMERCE_POSTGRES_USER",
        "ECOMMERCE_POSTGRES_PASSWORD",
        "ECOMMERCE_POSTGRES_DB",
        "ICEBERG_WAREHOUSE",
        "S3_ENDPOINT",
    )
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    # This JDBC URI configures the Iceberg *catalog*.  It is separate from the
    # JDBC reads of staging tables performed by sales_batch.py.
    jdbc_uri = (
        f"jdbc:postgresql://{os.environ['ECOMMERCE_POSTGRES_HOST']}:"
        f"{os.environ['ECOMMERCE_POSTGRES_PORT']}/"
        f"{os.environ['ECOMMERCE_POSTGRES_DB']}"
    )
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # `local` becomes the first segment in identifiers such as
        # local.ecommerce.fact_sales.
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "jdbc")
        .config("spark.sql.catalog.local.uri", jdbc_uri)
        .config(
            "spark.sql.catalog.local.jdbc.user",
            os.environ["ECOMMERCE_POSTGRES_USER"],
        )
        .config(
            "spark.sql.catalog.local.jdbc.password",
            os.environ["ECOMMERCE_POSTGRES_PASSWORD"],
        )
        .config("spark.sql.catalog.local.warehouse", os.environ["ICEBERG_WAREHOUSE"])
        .config(
            "spark.sql.catalog.local.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.local.s3.endpoint", os.environ["S3_ENDPOINT"])
        # MinIO needs path-style URLs locally (`endpoint/bucket/key`) rather
        # than AWS virtual-host bucket DNS.
        .config("spark.sql.catalog.local.s3.path-style-access", "true")
        .config("spark.sql.session.timeZone", "UTC")
        # The local worker is deliberately small.  Keeping shuffle fan-out
        # bounded avoids hundreds of tiny tasks and leaves enough heap for an
        # Iceberg commit that touches a large initial snapshot.
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.default.parallelism", "64")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
