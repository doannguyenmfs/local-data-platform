"""Shared Spark/Iceberg catalog configuration."""

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
        .config("spark.sql.catalog.local.s3.path-style-access", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
