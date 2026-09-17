"""Consume versioned Kafka order events into three idempotent Iceberg views.

One Structured Streaming query produces: immutable logical event history,
latest order state and a dead-letter table.  Spark checkpointing records source
progress, while stable-key Iceberg MERGE closes the crash window where a sink
commit succeeds but checkpoint progress has not yet been persisted.

The module keeps parsing as a separate DataFrame transformation so its schema
contract can be unit-tested without starting Kafka or MinIO.
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from iceberg_common import iceberg_spark


EVENTS_TABLE = "local.ecommerce.order_events"
CURRENT_ORDERS_TABLE = "local.ecommerce.current_orders"
DEAD_LETTER_TABLE = "local.ecommerce.dead_letter_events"

# An explicit schema is safer than inference for a continuous stream: malformed
# JSON becomes a null parsed struct and is routed to DLQ instead of changing the
# DataFrame schema from batch to batch.
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("schema_version", IntegerType()),
        StructField("occurred_at", StringType()),
        StructField("produced_at", StringType()),
        StructField(
            "data",
            StructType(
                [
                    StructField("order_id", StringType()),
                    StructField("customer_id", StringType()),
                    StructField("status", StringType()),
                    StructField("total_amount", DecimalType(14, 2)),
                    StructField("order_date", StringType()),
                    StructField("source_updated_at", StringType()),
                ]
            ),
        ),
    ]
)


def parse_kafka_events(kafka_rows: DataFrame) -> DataFrame:
    """Parse the envelope while preserving Kafka coordinates for audit/dedupe."""
    # Preserve raw payload and Kafka coordinates before parsing.  These fields
    # are the audit trail and provide a deterministic DLQ identity.
    return (
        kafka_rows.select(
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("raw_value"),
            "topic",
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("event", F.from_json("raw_value", EVENT_SCHEMA))
        .select(
            "message_key",
            "raw_value",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            F.col("event.event_id").alias("event_id"),
            F.col("event.event_type").alias("event_type"),
            F.col("event.schema_version").alias("schema_version"),
            F.to_timestamp("event.occurred_at").alias("occurred_at"),
            F.to_timestamp("event.produced_at").alias("produced_at"),
            F.col("event.data.order_id").alias("order_id"),
            F.col("event.data.customer_id").alias("customer_id"),
            F.col("event.data.status").alias("status"),
            F.col("event.data.total_amount").alias("total_amount"),
            F.to_timestamp("event.data.order_date").alias("order_date"),
            F.to_timestamp("event.data.source_updated_at").alias(
                "source_updated_at"
            ),
        )
        .withColumn("ingested_at", F.current_timestamp())
        # Validation is deliberately conservative.  Unsupported event versions
        # are retained in DLQ rather than silently interpreted with v1 rules.
        .withColumn(
            "is_valid",
            F.col("event_id").isNotNull()
            & (F.col("event_type") == "order.upserted")
            & (F.col("schema_version") == 1)
            & F.col("order_id").isNotNull()
            & F.col("source_updated_at").isNotNull(),
        )
    )


def create_tables(spark) -> None:
    """Create/evolve the three sink contracts without dropping prior state."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.ecommerce")
    # Event history is partitioned by occurrence day for time-range scans.  Its
    # logical immutability is enforced by MERGE `WHEN NOT MATCHED` below.
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {EVENTS_TABLE} (
            event_id STRING NOT NULL,
            event_type STRING NOT NULL,
            schema_version INT NOT NULL,
            occurred_at TIMESTAMP,
            produced_at TIMESTAMP,
            order_id STRING NOT NULL,
            customer_id STRING,
            status STRING,
            total_amount DECIMAL(14, 2),
            order_date TIMESTAMP,
            source_updated_at TIMESTAMP NOT NULL,
            message_key STRING,
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            ingested_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(occurred_at))
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    # Current state is update-heavy, so merge-on-read avoids rewriting complete
    # data files for every micro-batch.  Maintenance later compacts delete files.
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {CURRENT_ORDERS_TABLE} (
            order_id STRING NOT NULL,
            customer_id STRING,
            status STRING,
            total_amount DECIMAL(14, 2),
            order_date TIMESTAMP,
            source_updated_at TIMESTAMP NOT NULL,
            latest_event_id STRING NOT NULL,
            updated_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.merge.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read'
        )
        """
    )
    # CREATE TABLE IF NOT EXISTS does not update properties of an existing
    # deployment. Keep this migration idempotent so older local volumes move
    # to the streaming-friendly row-level write mode automatically.
    spark.sql(
        f"""
        ALTER TABLE {CURRENT_ORDERS_TABLE} SET TBLPROPERTIES (
            'write.merge.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read'
        )
        """
    )
    # DLQ partitioning follows ingestion time because malformed payloads may not
    # contain a usable business timestamp.
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {DEAD_LETTER_TABLE} (
            dead_letter_id STRING NOT NULL,
            raw_value STRING,
            error_reason STRING,
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            ingested_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(ingested_at))
        TBLPROPERTIES ('format-version' = '2')
        """
    )


def merge_batch(batch: DataFrame, batch_id: int) -> None:
    """Commit one micro-batch to history/current/DLQ with stable identities.

    ``foreachBatch`` may invoke this function again for a previously committed
    offset range after a crash.  All writes therefore converge to the same
    final state instead of relying on exactly-once invocation.
    """
    if not batch.head(1):
        return
    # The same parsed rows feed up to three actions/sinks; persist avoids
    # re-reading Kafka source partitions for each branch.
    batch = batch.persist()
    spark = batch.sparkSession
    try:
        # In-batch dedupe handles repeated event IDs in the same offset range;
        # Iceberg MERGE handles duplicates across batches and restarts.
        valid = batch.filter("is_valid").dropDuplicates(["event_id"])
        # Hashing immutable Kafka coordinates creates a replay-stable DLQ key
        # even when the payload is not parseable enough to contain an event ID.
        invalid = (
            batch.filter("NOT is_valid")
            .withColumn(
                "dead_letter_id",
                F.sha2(
                    F.concat_ws(
                        ":", "topic", F.col("partition"), F.col("offset")
                    ),
                    256,
                ),
            )
            .withColumn(
                "error_reason",
                F.lit("invalid_json_or_unsupported_order_event_v1"),
            )
        )

        if valid.head(1):
            event_updates = valid.select(
                "event_id",
                "event_type",
                "schema_version",
                "occurred_at",
                "produced_at",
                "order_id",
                "customer_id",
                "status",
                "total_amount",
                "order_date",
                "source_updated_at",
                "message_key",
                F.col("topic").alias("kafka_topic"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                "kafka_timestamp",
                "ingested_at",
            )
            # Initial append avoids an expensive MERGE plan against an empty
            # table.  All later writes use event_id MERGE for replay safety.
            if spark.table(EVENTS_TABLE).limit(1).count() == 0:
                event_updates.writeTo(EVENTS_TABLE).append()
            else:
                event_updates.createOrReplaceTempView("order_event_updates")
                spark.sql(
                    f"""
                    MERGE INTO {EVENTS_TABLE} AS target
                    USING order_event_updates AS source
                      ON target.event_id = source.event_id
                    WHEN NOT MATCHED THEN INSERT *
                    """
                )

            # A micro-batch can hold several versions of one order.  Choose the
            # newest source version, breaking equal timestamps by Kafka offset.
            latest_window = Window.partitionBy("order_id").orderBy(
                F.col("source_updated_at").desc(),
                F.col("offset").desc(),
            )
            current_updates = (
                valid.withColumn("row_number", F.row_number().over(latest_window))
                .filter("row_number = 1")
                .select(
                    "order_id",
                    "customer_id",
                    "status",
                    "total_amount",
                    "order_date",
                    "source_updated_at",
                    F.col("event_id").alias("latest_event_id"),
                    F.col("ingested_at").alias("updated_at"),
                )
            )
            # The MATCHED predicate also rejects a late/older event so replay or
            # out-of-order arrival cannot roll current state backward.
            if spark.table(CURRENT_ORDERS_TABLE).limit(1).count() == 0:
                current_updates.writeTo(CURRENT_ORDERS_TABLE).append()
            else:
                current_updates.createOrReplaceTempView("current_order_updates")
                spark.sql(
                    f"""
                    MERGE INTO {CURRENT_ORDERS_TABLE} AS target
                    USING current_order_updates AS source
                      ON target.order_id = source.order_id
                    WHEN MATCHED
                      AND source.source_updated_at >= target.source_updated_at
                      THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                    """
                )

        if invalid.head(1):
            invalid_updates = invalid.select(
                "dead_letter_id",
                "raw_value",
                "error_reason",
                F.col("topic").alias("kafka_topic"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                "kafka_timestamp",
                "ingested_at",
            )
            # Kafka-coordinate MERGE guarantees one DLQ row per poison record.
            if spark.table(DEAD_LETTER_TABLE).limit(1).count() == 0:
                invalid_updates.writeTo(DEAD_LETTER_TABLE).append()
            else:
                invalid_updates.createOrReplaceTempView("dead_letter_updates")
                spark.sql(
                    f"""
                    MERGE INTO {DEAD_LETTER_TABLE} AS target
                    USING dead_letter_updates AS source
                      ON target.dead_letter_id = source.dead_letter_id
                    WHEN NOT MATCHED THEN INSERT *
                    """
                )
        print(f"STREAM_BATCH_COMMITTED batch_id={batch_id}")
    finally:
        batch.unpersist()


def parse_args() -> argparse.Namespace:
    """Switch between continuous service mode and finite backlog processing."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--available-now", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    topic = os.getenv("KAFKA_ORDER_TOPIC", "ecommerce.order-events.v1")
    checkpoint = os.getenv(
        "SPARK_STREAM_CHECKPOINT",
        "/opt/lakehouse/checkpoints/order-events-v1",
    )
    spark = iceberg_spark("ecommerce-order-stream")
    spark.sparkContext.setLogLevel("WARN")
    create_tables(spark)

    # `startingOffsets=earliest` only applies when no checkpoint exists.  Once
    # created, checkpoint progress is authoritative.  failOnDataLoss surfaces a
    # retention gap instead of silently skipping offsets.
    kafka_rows = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", "10000")
        .load()
    )
    # foreachBatch provides DataFrame batch semantics and lets all three Iceberg
    # tables participate in replay-safe MERGE logic.  The three commits are not
    # one atomic transaction; stable keys make retry converge if one fails.
    writer = (
        parse_kafka_events(kafka_rows)
        .writeStream.foreachBatch(merge_batch)
        .option("checkpointLocation", checkpoint)
        .queryName("order-events-to-iceberg")
    )
    # availableNow is ideal for tests/backlog drains: process all offsets visible
    # at start and stop.  Service mode polls every 10 seconds indefinitely.
    query = (
        writer.trigger(availableNow=True).start()
        if args.available_now
        else writer.trigger(processingTime="10 seconds").start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
