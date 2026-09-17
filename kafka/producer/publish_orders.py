"""Publish deterministic order events from staging for replayable local demos."""

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


EVENT_NAMESPACE = uuid.UUID("c9769ea0-bba4-4ab6-9e12-73d9199cf5c7")


def json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    return parser.parse_args()


def required(name: str) -> str:
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
    query = """
        SELECT
            order_id,
            customer_id,
            status,
            total_amount,
            order_date,
            updated_at
        FROM staging.orders
        ORDER BY updated_at, order_id
    """
    parameters: tuple[Any, ...] = ()
    if args.limit is not None:
        query += " LIMIT %s"
        parameters = (args.limit,)

    delivered = 0
    delivery_errors: list[str] = []

    def delivery_callback(error, message) -> None:
        nonlocal delivered
        if error:
            delivery_errors.append(str(error))
            return
        delivered += 1

    with connection, connection.cursor() as cursor:
        cursor.execute(query, parameters)
        columns = [column.name for column in cursor.description]
        for values in cursor:
            row = dict(zip(columns, values))
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
