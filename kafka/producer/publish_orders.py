"""Publish deterministic order events from staging.

The producer is a finite batch hand-off, not the long-running stream.  It reads
exactly the Airflow candidate window (or an explicit backfill window), builds a
versioned event envelope and waits for broker delivery acknowledgements before
returning success.  Stable UUID5 identities make a later Airflow retry safe
across producer processes, which library-level idempotence alone cannot do.
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg
from confluent_kafka import Producer


# UUID5(namespace, name) is deterministic.  Do not change this namespace after
# events have been published: doing so would assign a second identity to the
# same source version and defeat downstream replay deduplication.
EVENT_NAMESPACE = uuid.UUID("c9769ea0-bba4-4ab6-9e12-73d9199cf5c7")


def json_default(value: Any) -> str:
    """Serialize database-native scalar types without losing precision."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def parse_args() -> argparse.Namespace:
    """Accept an optional row cap and an all-or-nothing backfill interval."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--start")
    parser.add_argument("--end")
    return parser.parse_args()


def required(name: str) -> str:
    """Read mandatory runtime configuration without printing secret values."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def main() -> None:
    args = parse_args()
    topic = os.getenv("KAFKA_ORDER_TOPIC", "ecommerce.order-events.v1")
    producer = Producer(
        {
            "bootstrap.servers": required("KAFKA_BOOTSTRAP_SERVERS"),
            "client.id": "staging-order-producer",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "snappy",
            "linger.ms": 20,
        }
    )
    connection = psycopg.connect(
        host=required("ECOMMERCE_POSTGRES_HOST"),
        port=required("ECOMMERCE_POSTGRES_PORT"),
        user=required("ECOMMERCE_POSTGRES_USER"),
        password=required("ECOMMERCE_POSTGRES_PASSWORD"),
        dbname=required("ECOMMERCE_POSTGRES_DB"),
    )
    delivered = 0
    delivery_errors: list[str] = []

    def delivery_callback(error, message) -> None:
        """Run asynchronously when Kafka accepts or rejects a record."""
        nonlocal delivered
        if error:
            delivery_errors.append(str(error))
            return
        delivered += 1

    with connection, connection.cursor() as cursor:
        # Explicit backfills use [start, end).  Scheduled runs use the same
        # (committed, candidate] bounds that Airflow extracted into staging.
        if (args.start is None) != (args.end is None):
            raise ValueError("--start and --end must be supplied together")
        if args.start is not None:
            lower_bound = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
            upper_bound = datetime.fromisoformat(args.end.replace("Z", "+00:00"))
            lower_operator = ">="
            upper_operator = "<"
        else:
            cursor.execute(
                """
                SELECT watermark_value, candidate_value
                FROM metadata.etl_watermark
                WHERE pipeline_name = 'staging_orders'
                """
            )
            bounds = cursor.fetchone()
            if bounds is None or bounds[1] is None:
                raise ValueError("Missing staging_orders candidate watermark")
            lower_bound, upper_bound = bounds
            lower_operator = ">"
            upper_operator = "<="

        query = f"""
            SELECT
                order_id,
                customer_id,
                status,
                total_amount,
                order_date,
                updated_at
            FROM staging.orders
            WHERE updated_at {lower_operator} %s
              AND updated_at {upper_operator} %s
            ORDER BY updated_at, order_id
        """
        parameters: tuple[Any, ...] = (lower_bound, upper_bound)
        if args.limit is not None:
            query += " LIMIT %s"
            parameters = (*parameters, args.limit)

        # A server-side cursor is not required for the current data volume;
        # psycopg still yields rows one at a time below, avoiding a second large
        # Python list.  ORDER BY is for deterministic observability, not global
        # Kafka ordering (which exists only within a partition).
        cursor.execute(query, parameters)
        columns = [column.name for column in cursor.description]
        for values in cursor:
            row = dict(zip(columns, values))
            # One business order version maps to one event identity forever.
            event_id = uuid.uuid5(
                EVENT_NAMESPACE,
                f"{row['order_id']}:{row['updated_at'].isoformat()}",
            )
            event = {
                "event_id": str(event_id),
                "event_type": "order.upserted",
                "schema_version": 1,
                "occurred_at": row["order_date"],
                "produced_at": datetime.now(timezone.utc),
                "data": {
                    "order_id": row["order_id"],
                    "customer_id": row["customer_id"],
                    "status": row["status"],
                    "total_amount": row["total_amount"],
                    "order_date": row["order_date"],
                    "source_updated_at": row["updated_at"],
                },
            }
            # produce() is asynchronous and may fill its local buffer.  Polling
            # lets delivery callbacks run and applies backpressure rather than
            # dropping a record when BufferError is raised.
            while True:
                try:
                    producer.produce(
                        topic=topic,
                        key=str(row["order_id"]),
                        value=json.dumps(
                            event,
                            default=json_default,
                            separators=(",", ":"),
                        ),
                        on_delivery=delivery_callback,
                    )
                    break
                except BufferError:
                    producer.poll(1)
            producer.poll(0)

    # Enqueueing is not a successful hand-off.  Flush and callback checks make
    # task success mean that every event received a broker acknowledgement.
    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"{remaining} Kafka messages were not delivered")
    if delivery_errors:
        raise RuntimeError(
            f"{len(delivery_errors)} Kafka deliveries failed: {delivery_errors[0]}"
        )
    print(json.dumps({"topic": topic, "delivered_events": delivered}))


if __name__ == "__main__":
    main()
