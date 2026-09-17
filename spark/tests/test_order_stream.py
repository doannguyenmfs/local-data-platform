"""Unit tests for order-event envelope parsing without a Kafka broker."""

from __future__ import annotations

import json
import sys
import unittest
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession

sys.path.insert(0, "/opt/spark/jobs")

from order_stream import parse_kafka_events  # noqa: E402


class OrderStreamParsingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("order-stream-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_valid_and_invalid_envelopes_are_classified(self) -> None:
        valid = {
            "event_id": "evt-1",
            "event_type": "order.upserted",
            "schema_version": 1,
            "occurred_at": "2026-01-02T10:00:00+00:00",
            "produced_at": "2026-01-02T10:01:00+00:00",
            "data": {
                "order_id": "order-1",
                "customer_id": "customer-1",
                "status": "confirmed",
                "total_amount": 10.0,
                "order_date": "2026-01-02T10:00:00+00:00",
                "source_updated_at": "2026-01-02T10:00:30+00:00",
            },
        }
        now = datetime.now(timezone.utc)
        rows = [
            Row(
                key=b"order-1",
                value=json.dumps(valid).encode(),
                topic="orders",
                partition=0,
                offset=1,
                timestamp=now,
            ),
            Row(
                key=b"broken",
                value=b"not-json",
                topic="orders",
                partition=0,
                offset=2,
                timestamp=now,
            ),
        ]
        parsed = parse_kafka_events(self.spark.createDataFrame(rows))

        self.assertEqual(parsed.filter("is_valid").count(), 1)
        self.assertEqual(parsed.filter("NOT is_valid").count(), 1)
        self.assertEqual(parsed.filter("is_valid").first().order_id, "order-1")


if __name__ == "__main__":
    unittest.main(verbosity=2)
