"""End-to-end CDC smoke test for customer create, update and delete events.

The test starts a Kafka consumer at the current topic tail, mutates one isolated
customer in three committed PostgreSQL transactions, and waits for Debezium to
emit c/u/d plus the delete tombstone.  It leaves no source row behind.

This proves CAPTURE + durable Kafka storage only.  It does not consume the event
into staging or dbt, so it must not be interpreted as downstream delete APPLY.
"""

from __future__ import annotations

import os
import time
import uuid

import psycopg
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def assign_topic_tail(consumer: Consumer, topic: str) -> None:
    """Pin every current partition to its tail before source mutations.

    ``subscribe()`` involves asynchronous group assignment and offset reset. A
    partition can appear assigned before its ``latest`` position is resolved,
    leaving a narrow race where a very fast test mutation is skipped. This
    one-process diagnostic needs no rebalancing. It resolves each partition's
    concrete high watermark before the mutation instead of assigning symbolic
    OFFSET_END, which the client may resolve lazily on first poll.
    """
    metadata = consumer.list_topics(topic=topic, timeout=15)
    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None or topic_metadata.error is not None:
        raise RuntimeError(f"CDC topic is unavailable: {topic}")
    tail_positions = []
    for partition_id in sorted(topic_metadata.partitions):
        partition = TopicPartition(topic, partition_id)
        _low, high = consumer.get_watermark_offsets(partition, timeout=15)
        tail_positions.append(TopicPartition(topic, partition_id, high))
    consumer.assign(tail_positions)


def mutate_customer(customer_id: uuid.UUID) -> None:
    """Commit three separate transactions so CDC must expose all operations."""
    connection_args = {
        "host": required("ECOMMERCE_POSTGRES_HOST"),
        "port": required("ECOMMERCE_POSTGRES_PORT"),
        "user": required("ECOMMERCE_POSTGRES_USER"),
        "password": required("ECOMMERCE_POSTGRES_PASSWORD"),
        "dbname": required("ECOMMERCE_POSTGRES_DB"),
    }
    with psycopg.connect(**connection_args) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO public.customers (
                    customer_id, first_name, last_name, email
                ) VALUES (%s, %s, %s, %s)
                """,
                (
                    customer_id,
                    "CDC",
                    "Created",
                    f"cdc-smoke-{customer_id}@example.test",
                ),
            )
        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE public.customers
                SET last_name = 'Updated', updated_at = clock_timestamp()
                WHERE customer_id = %s
                """,
                (customer_id,),
            )
        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM public.customers WHERE customer_id = %s",
                (customer_id,),
            )
        connection.commit()


def is_target_key(
    raw_key: bytes | None,
    customer_id: uuid.UUID,
    topic: str,
    deserializer: AvroDeserializer,
) -> bool:
    """Decode a registry-backed Avro key and match the isolated test UUID."""
    if raw_key is None:
        return False
    key = deserializer(raw_key, SerializationContext(topic, MessageField.KEY))
    return str(key.get("customer_id")) == str(customer_id)


def main() -> None:
    customer_id = uuid.uuid4()
    topic = f"{required('CDC_TOPIC_PREFIX')}.public.customers"
    # Apicurio exposes the Confluent-compatible API, so the standard Python
    # deserializer can resolve schema IDs embedded in the Avro wire payload.
    registry_client = SchemaRegistryClient(
        {"url": required("SCHEMA_REGISTRY_CCOMPAT_URL")}
    )
    key_deserializer = AvroDeserializer(registry_client)
    value_deserializer = AvroDeserializer(registry_client)
    consumer = Consumer(
        {
            "bootstrap.servers": required("KAFKA_BOOTSTRAP_SERVERS"),
            "group.id": f"cdc-smoke-{customer_id}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    try:
        # Establish the observation boundary first. Every matching message seen
        # afterwards must have been caused by this test, not the initial snapshot.
        assign_topic_tail(consumer, topic)
        mutate_customer(customer_id)

        operations: list[str] = []
        saw_tombstone = False
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())
            if not is_target_key(
                message.key(), customer_id, topic, key_deserializer
            ):
                continue
            if message.value() is None:
                saw_tombstone = True
            else:
                event = value_deserializer(
                    message.value(),
                    SerializationContext(topic, MessageField.VALUE),
                )
                operation = event.get("op")
                if operation in {"c", "u", "d"}:
                    operations.append(operation)
            if operations == ["c", "u", "d"] and saw_tombstone:
                print(
                    "CDC smoke test passed: operations=c,u,d; "
                    "delete_tombstone=true; "
                    f"customer_id={customer_id}"
                )
                return
        raise TimeoutError(
            "Timed out waiting for customer CDC sequence; "
            f"operations={operations}, tombstone={saw_tombstone}"
        )
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
