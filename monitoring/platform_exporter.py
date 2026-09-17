"""Expose platform health and pipeline semantics as Prometheus metrics.

The exporter intentionally survives dependency failures.  A monitoring process
that exits whenever Kafka/PostgreSQL is down cannot report *which* dependency
failed.  Each collector therefore converts its own exception into component
and collection-error gauges while the HTTP metrics endpoint stays available.

Beginner map: collectors READ system state; Gauges translate it into numbers;
Prometheus SCRAPES those numbers and evaluates alert rules.  This process never
repairs a pipeline or sends notifications itself.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Optional

import psycopg
import requests
from confluent_kafka.admin import AdminClient
from prometheus_client import Gauge, start_http_server


# Labels are bounded component/pipeline/table names.  Business identifiers are
# intentionally excluded because unbounded labels create a time-series cardinality
# explosion in Prometheus.
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
CDC_CONNECTOR_UP = Gauge(
    "cdc_connector_up",
    "Whether the Debezium connector and every connector task are RUNNING.",
    ["connector"],
)
CDC_SLOT_ACTIVE = Gauge(
    "cdc_replication_slot_active",
    "Whether PostgreSQL reports the logical replication slot as active.",
    ["slot"],
)
CDC_SLOT_RETAINED_BYTES = Gauge(
    "cdc_replication_slot_retained_bytes",
    "Approximate WAL bytes retained from a logical slot restart LSN.",
    ["slot"],
)
COLLECTION_ERRORS = Gauge(
    "platform_exporter_collection_errors",
    "Collection failures by subsystem during the latest cycle.",
    ["subsystem"],
)


def env(name: str, default: Optional[str] = None) -> str:
    """Return mandatory config while keeping credentials out of log output."""
    value = os.getenv(name, default)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def collect_postgres() -> None:
    """Collect committed freshness, pending batches and staging volume."""
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
                # Freshness is measured from the committed watermark.  Candidate
                # state is not yet safe for consumers and has its own gauge.
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

                # A replication slot intentionally prevents PostgreSQL from
                # deleting unread WAL. This is CDC durability, but an inactive
                # or lagging slot can also fill the source database disk.
                slot = env("CDC_SLOT_NAME", "ecommerce_cdc_slot")
                cursor.execute(
                    """
                    SELECT
                        active,
                        COALESCE(
                            pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),
                            0
                        )
                    FROM pg_replication_slots
                    WHERE slot_name = %s
                    """,
                    (slot,),
                )
                slot_state = cursor.fetchone()
                CDC_SLOT_ACTIVE.labels(slot).set(
                    1 if slot_state is not None and slot_state[0] else 0
                )
                CDC_SLOT_RETAINED_BYTES.labels(slot).set(
                    float(slot_state[1]) if slot_state is not None else 0
                )
        COMPONENT_UP.labels("postgres").set(1)
        COLLECTION_ERRORS.labels("postgres").set(0)
    except Exception as error:  # exporter must stay alive and expose failure
        print(f"postgres collection failed: {type(error).__name__}: {error}")
        COMPONENT_UP.labels("postgres").set(0)
        COLLECTION_ERRORS.labels("postgres").set(1)


def collect_http(component: str, url: str) -> None:
    """Convert one dependency's HTTP liveness endpoint into semantic health."""
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
    """Verify the required topic exists and expose its bounded partition count."""
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


def collect_debezium() -> None:
    """Check connector/task state, not merely the Kafka Connect HTTP port."""
    # HTTP 200 from the worker only proves the worker process answers. A source
    # task can still be FAILED, so health requires connector + every task to be
    # RUNNING. WAL retention is checked separately in collect_postgres().
    connector = env("CDC_CONNECTOR_NAME", "ecommerce-postgres-cdc")
    base_url = env("DEBEZIUM_CONNECT_URL", "http://debezium-connect:8083")
    try:
        response = requests.get(
            f"{base_url.rstrip('/')}/connectors/{connector}/status",
            timeout=5,
        )
        response.raise_for_status()
        status = response.json()
        tasks = status.get("tasks", [])
        running = (
            status.get("connector", {}).get("state") == "RUNNING"
            and bool(tasks)
            and all(task.get("state") == "RUNNING" for task in tasks)
        )
        CDC_CONNECTOR_UP.labels(connector).set(1 if running else 0)
        COMPONENT_UP.labels("debezium").set(1 if running else 0)
        COLLECTION_ERRORS.labels("debezium").set(0)
    except Exception as error:
        print(f"debezium collection failed: {type(error).__name__}: {error}")
        CDC_CONNECTOR_UP.labels(connector).set(0)
        COMPONENT_UP.labels("debezium").set(0)
        COLLECTION_ERRORS.labels("debezium").set(1)


def collect() -> None:
    """Run collectors independently so one subsystem cannot hide the others."""
    collect_postgres()
    collect_kafka()
    collect_debezium()
    collect_http("spark_gateway", env("SPARK_GATEWAY_HEALTH_URL"))
    collect_http("minio", env("MINIO_HEALTH_URL"))
    collect_http("airflow", env("AIRFLOW_HEALTH_URL"))


def main() -> None:
    port = int(env("EXPORTER_PORT", "9100"))
    interval = int(env("COLLECTION_INTERVAL_SECONDS", "15"))
    start_http_server(port)
    print(f"Platform exporter listening on :{port}/metrics")
    # Collection happens on a fixed cadence independent from Prometheus scrape
    # timing.  Scrapes remain cheap and do not fan out to every dependency.
    while True:
        started_at = time.monotonic()
        collect()
        time.sleep(max(1.0, interval - (time.monotonic() - started_at)))


if __name__ == "__main__":
    main()
