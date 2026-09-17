"""Expose platform health and data-pipeline state as Prometheus metrics."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Optional

import psycopg
import requests
from confluent_kafka.admin import AdminClient
from prometheus_client import Gauge, start_http_server


COMPONENT_UP = Gauge(
    "platform_component_up",
    "Whether the platform component passed its semantic health check.",
    ["component"],
)
WATERMARK_LAG = Gauge(
    "pipeline_watermark_lag_seconds",
    "Seconds between now and the committed source watermark.",
    ["pipeline"],
)
CANDIDATE_PENDING = Gauge(
    "pipeline_candidate_pending",
    "Whether a candidate watermark is waiting for downstream commits.",
    ["pipeline"],
)
STAGING_ROWS = Gauge(
    "staging_table_rows",
    "Current row count in a staging table.",
    ["table"],
)
KAFKA_PARTITIONS = Gauge(
    "kafka_topic_partitions",
    "Partition count for a required Kafka topic.",
    ["topic"],
)
COLLECTION_ERRORS = Gauge(
    "platform_exporter_collection_errors",
    "Collection failures by subsystem during the latest cycle.",
    ["subsystem"],
)


def env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def collect_postgres() -> None:
    try:
        with psycopg.connect(
            host=env("ECOMMERCE_POSTGRES_HOST"),
            port=env("ECOMMERCE_POSTGRES_PORT"),
            user=env("ECOMMERCE_POSTGRES_USER"),
            password=env("ECOMMERCE_POSTGRES_PASSWORD"),
            dbname=env("ECOMMERCE_POSTGRES_DB"),
            connect_timeout=5,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT pipeline_name, watermark_value, candidate_value
                    FROM metadata.etl_watermark
                    """
                )
                now = datetime.now(timezone.utc)
                for pipeline, watermark, candidate in cursor:
                    WATERMARK_LAG.labels(pipeline).set(
                        max(0.0, (now - watermark).total_seconds())
                    )
                    CANDIDATE_PENDING.labels(pipeline).set(
                        1 if candidate is not None else 0
                    )

                for table in (
                    "customers",
                    "products",
                    "orders",
                    "order_items",
                    "payments",
                ):
                    cursor.execute(f"SELECT COUNT(*) FROM staging.{table}")
                    STAGING_ROWS.labels(table).set(cursor.fetchone()[0])
        COMPONENT_UP.labels("postgres").set(1)
        COLLECTION_ERRORS.labels("postgres").set(0)
    except Exception as error:  # exporter must stay alive and expose failure
        print(f"postgres collection failed: {type(error).__name__}: {error}")
        COMPONENT_UP.labels("postgres").set(0)
        COLLECTION_ERRORS.labels("postgres").set(1)


def collect_http(component: str, url: str) -> None:
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        COMPONENT_UP.labels(component).set(1)
        COLLECTION_ERRORS.labels(component).set(0)
    except Exception as error:
        print(f"{component} collection failed: {type(error).__name__}: {error}")
        COMPONENT_UP.labels(component).set(0)
        COLLECTION_ERRORS.labels(component).set(1)


def collect_kafka() -> None:
    topic = env("KAFKA_ORDER_TOPIC", "ecommerce.order-events.v1")
    try:
        admin = AdminClient(
            {"bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")}
        )
        metadata = admin.list_topics(topic=topic, timeout=5)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"Required topic is unavailable: {topic}")
        KAFKA_PARTITIONS.labels(topic).set(len(topic_metadata.partitions))
        COMPONENT_UP.labels("kafka").set(1)
        COLLECTION_ERRORS.labels("kafka").set(0)
    except Exception as error:
        print(f"kafka collection failed: {type(error).__name__}: {error}")
        KAFKA_PARTITIONS.labels(topic).set(0)
        COMPONENT_UP.labels("kafka").set(0)
        COLLECTION_ERRORS.labels("kafka").set(1)


def collect() -> None:
    collect_postgres()
    collect_kafka()
    collect_http("spark_gateway", env("SPARK_GATEWAY_HEALTH_URL"))
    collect_http("minio", env("MINIO_HEALTH_URL"))
    collect_http("airflow", env("AIRFLOW_HEALTH_URL"))


def main() -> None:
    port = int(env("EXPORTER_PORT", "9100"))
    interval = int(env("COLLECTION_INTERVAL_SECONDS", "15"))
    start_http_server(port)
    print(f"Platform exporter listening on :{port}/metrics")
    while True:
        started_at = time.monotonic()
        collect()
        time.sleep(max(1.0, interval - (time.monotonic() - started_at)))


if __name__ == "__main__":
    main()
