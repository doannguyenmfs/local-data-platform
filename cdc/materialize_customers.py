"""Materialize Debezium customer events into replayable Bronze and Silver.

Beginner map
------------
1. Kafka retains the ordered Avro records produced by Debezium.
2. Bronze stores each physical Kafka record once, identified by topic/partition/
   offset. It is immutable evidence, not a business current-state table.
3. Silver folds r/c/u/d events into one row per customer and preserves delete as
   ``is_deleted=true``. ``customers_current`` hides those tombstones from dbt.
4. PostgreSQL commits before Kafka offsets. A crash in between replays records;
   Bronze's primary key makes that replay a safe no-op before offsets advance.

This is at-least-once transport plus idempotent database application. It does
not claim a distributed exactly-once transaction between Kafka and PostgreSQL.

After P0 cutover this process is the only writer of CDC customer Silver state;
Airflow no longer polls customers. Keeping that ownership singular prevents a
late batch UPSERT from resurrecting a customer that CDC has already deleted.
This service materializes data only: alert delivery, backup and automatic
repair remain separate operational responsibilities.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from psycopg.types.json import Jsonb


BATCH_SIZE = 500
MIGRATION = Path("/app/postgres/sql/003_cdc_customer_layers.sql")
RUNNING = True


def required(name: str) -> str:
    """Read mandatory runtime configuration without printing secrets."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def json_default(value: Any) -> str:
    """Make decoded Avro values safe for PostgreSQL JSONB audit columns."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (UUID, Decimal)):
        return str(value)
    raise TypeError(f"Cannot JSON encode {type(value).__name__}")


def as_jsonb(value: Any) -> Jsonb | None:
    """Wrap nested values using one deterministic encoder."""
    if value is None:
        return None
    normalized = json.loads(json.dumps(value, default=json_default))
    return Jsonb(normalized)


def database_args() -> dict[str, str]:
    return {
        "host": required("ECOMMERCE_POSTGRES_HOST"),
        "port": required("ECOMMERCE_POSTGRES_PORT"),
        "user": required("ECOMMERCE_POSTGRES_USER"),
        "password": required("ECOMMERCE_POSTGRES_PASSWORD"),
        "dbname": required("ECOMMERCE_POSTGRES_DB"),
    }


def apply_migration(connection: psycopg.Connection) -> None:
    """Converge database objects before consuming any record."""
    connection.execute(MIGRATION.read_text(encoding="utf-8"))
    connection.commit()


def parse_timestamp(value: Any) -> Any:
    """Let psycopg bind ISO timestamps while preserving nullable fields."""
    return value


def kafka_timestamp(message: Any) -> datetime | None:
    """Convert Kafka epoch milliseconds to a timezone-aware timestamp."""
    _timestamp_type, milliseconds = message.timestamp()
    if milliseconds is None or milliseconds < 0:
        return None
    return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)


def insert_bronze(
    cursor: psycopg.Cursor,
    *,
    message: Any,
    key: dict[str, Any],
    event: dict[str, Any] | None,
) -> bool:
    """Insert immutable evidence and return False for an already-seen offset."""
    source = (event or {}).get("source") or {}
    transaction = (event or {}).get("transaction") or {}
    operation = (event or {}).get("op") if event is not None else "t"
    if operation not in {"r", "c", "u", "d", "t"}:
        raise ValueError(f"Unsupported Debezium operation: {operation!r}")

    cursor.execute(
        """
        INSERT INTO cdc_bronze.customer_changes (
            kafka_topic, kafka_partition, kafka_offset, customer_id,
            operation, source_lsn, source_ts_ms, transaction_id,
            event_key, before_value, after_value, kafka_timestamp
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
        RETURNING 1
        """,
        (
            message.topic(),
            message.partition(),
            message.offset(),
            key["customer_id"],
            operation,
            source.get("lsn"),
            source.get("ts_ms"),
            transaction.get("id"),
            as_jsonb(key),
            as_jsonb((event or {}).get("before")),
            as_jsonb((event or {}).get("after")),
            kafka_timestamp(message),
        ),
    )
    return cursor.fetchone() is not None


def apply_silver(
    cursor: psycopg.Cursor,
    *,
    message: Any,
    key: dict[str, Any],
    event: dict[str, Any] | None,
) -> None:
    """Fold one new Bronze event into latest customer state.

    Tombstones carry no payload and therefore do not change Silver; the prior
    ``d`` event already marked the key deleted. The ordering predicate protects
    Silver from late retries after a newer source LSN has been applied.
    """
    if event is None:
        return

    operation = event.get("op")
    source = event.get("source") or {}
    source_lsn = source.get("lsn")
    if source_lsn is None:
        raise ValueError("Debezium data event is missing source.lsn")

    common = (
        operation,
        source_lsn,
        source.get("ts_ms"),
        message.topic(),
        message.partition(),
        message.offset(),
    )

    if operation in {"r", "c", "u"}:
        after = event.get("after")
        if not after:
            raise ValueError(f"Debezium {operation} event is missing after")
        cursor.execute(
            """
            INSERT INTO cdc_silver.customers (
                customer_id, first_name, last_name, email,
                created_at, updated_at, is_deleted,
                source_operation, source_lsn, source_ts_ms,
                kafka_topic, kafka_partition, kafka_offset
            )
            VALUES (%s, %s, %s, %s, %s, %s, FALSE, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                is_deleted = FALSE,
                source_operation = EXCLUDED.source_operation,
                source_lsn = EXCLUDED.source_lsn,
                source_ts_ms = EXCLUDED.source_ts_ms,
                kafka_topic = EXCLUDED.kafka_topic,
                kafka_partition = EXCLUDED.kafka_partition,
                kafka_offset = EXCLUDED.kafka_offset,
                applied_at = clock_timestamp()
            WHERE (EXCLUDED.source_lsn, EXCLUDED.kafka_offset)
                > (customers.source_lsn, customers.kafka_offset)
            """,
            (
                after["customer_id"],
                after.get("first_name"),
                after.get("last_name"),
                after.get("email"),
                parse_timestamp(after.get("created_at")),
                parse_timestamp(after.get("updated_at")),
                *common,
            ),
        )
    elif operation == "d":
        # Preserve attributes from an existing row. If retention/recovery starts
        # at a delete without its create, the tombstone still records that the
        # business key is not current instead of resurrecting it.
        cursor.execute(
            """
            INSERT INTO cdc_silver.customers (
                customer_id, is_deleted, source_operation, source_lsn,
                source_ts_ms, kafka_topic, kafka_partition, kafka_offset
            )
            VALUES (%s, TRUE, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO UPDATE SET
                is_deleted = TRUE,
                source_operation = EXCLUDED.source_operation,
                source_lsn = EXCLUDED.source_lsn,
                source_ts_ms = EXCLUDED.source_ts_ms,
                kafka_topic = EXCLUDED.kafka_topic,
                kafka_partition = EXCLUDED.kafka_partition,
                kafka_offset = EXCLUDED.kafka_offset,
                applied_at = clock_timestamp()
            WHERE (EXCLUDED.source_lsn, EXCLUDED.kafka_offset)
                > (customers.source_lsn, customers.kafka_offset)
            """,
            (key["customer_id"], *common),
        )


def commit_offsets(consumer: Consumer, messages: list[Any]) -> None:
    """Commit next offsets for every partition represented in the DB batch."""
    next_offsets: dict[tuple[str, int], int] = {}
    for message in messages:
        key = (message.topic(), message.partition())
        next_offsets[key] = max(next_offsets.get(key, 0), message.offset() + 1)
    consumer.commit(
        offsets=[
            TopicPartition(topic, partition, offset)
            for (topic, partition), offset in next_offsets.items()
        ],
        asynchronous=False,
    )


def process_batch(
    connection: psycopg.Connection,
    consumer: Consumer,
    messages: list[Any],
    key_deserializer: AvroDeserializer,
    value_deserializer: AvroDeserializer,
) -> tuple[int, int]:
    """Atomically persist a polled batch, then acknowledge it to Kafka."""
    inserted = 0
    duplicates = 0
    with connection.cursor() as cursor:
        for message in messages:
            context_key = SerializationContext(message.topic(), MessageField.KEY)
            key = key_deserializer(message.key(), context_key)
            if not key or not key.get("customer_id"):
                raise ValueError("Customer CDC record has no business key")
            event = None
            if message.value() is not None:
                context_value = SerializationContext(
                    message.topic(), MessageField.VALUE
                )
                event = value_deserializer(message.value(), context_value)

            if insert_bronze(cursor, message=message, key=key, event=event):
                apply_silver(cursor, message=message, key=key, event=event)
                inserted += 1
            else:
                duplicates += 1

    # Deliberate ordering: data first, Kafka acknowledgement second. A crash
    # here causes replay, which the Bronze primary key absorbs on restart.
    connection.commit()
    commit_offsets(consumer, messages)
    return inserted, duplicates


def stop(_signum: int, _frame: Any) -> None:
    global RUNNING
    RUNNING = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--idle-exit-seconds",
        type=int,
        default=0,
        help="Exit after N idle seconds; zero keeps the production consumer alive.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    topic = f"{required('CDC_TOPIC_PREFIX')}.public.customers"
    registry = SchemaRegistryClient(
        {"url": required("SCHEMA_REGISTRY_CCOMPAT_URL")}
    )
    key_deserializer = AvroDeserializer(registry)
    value_deserializer = AvroDeserializer(registry)
    consumer = Consumer(
        {
            "bootstrap.servers": required("KAFKA_BOOTSTRAP_SERVERS"),
            "group.id": required("CDC_CUSTOMER_CONSUMER_GROUP"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "max.poll.interval.ms": 900000,
        }
    )

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    with psycopg.connect(**database_args()) as connection:
        apply_migration(connection)
        consumer.subscribe([topic])
        last_record_at = time.monotonic()
        total_inserted = 0
        total_duplicates = 0
        try:
            while RUNNING:
                messages = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
                valid_messages = []
                for message in messages:
                    if message is None:
                        continue
                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise RuntimeError(message.error())
                    valid_messages.append(message)

                if valid_messages:
                    inserted, duplicates = process_batch(
                        connection,
                        consumer,
                        valid_messages,
                        key_deserializer,
                        value_deserializer,
                    )
                    total_inserted += inserted
                    total_duplicates += duplicates
                    last_record_at = time.monotonic()
                    print(
                        f"CDC batch committed: records={len(valid_messages)}, "
                        f"inserted={inserted}, replayed={duplicates}",
                        flush=True,
                    )
                elif (
                    args.idle_exit_seconds > 0
                    and time.monotonic() - last_record_at >= args.idle_exit_seconds
                ):
                    break
        finally:
            consumer.close()

    print(
        "Customer CDC materialization stopped cleanly: "
        f"inserted={total_inserted}, replayed={total_duplicates}"
    )


if __name__ == "__main__":
    main()
