"""Executable unit tests for the pure Spark sales transformation."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/spark/jobs")

from sales_batch import transform_sales, validate_sales  # noqa: E402


class SalesBatchTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("sales-batch-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_transform_preserves_grain_and_derives_payment_state(self) -> None:
        timestamp = datetime(2026, 1, 2, 10, tzinfo=timezone.utc)
        orders = self.spark.createDataFrame(
            [("o1", "c1", timestamp, "delivered", timestamp)],
            "order_id string, customer_id string, order_date timestamp, "
            "status string, loaded_at timestamp",
        )
        items = self.spark.createDataFrame(
            [
                ("i1", "o1", "p1", 1, Decimal("10.00"), timestamp),
                ("i2", "o1", "p2", 2, Decimal("20.00"), timestamp),
            ],
            "order_item_id string, order_id string, product_id string, "
            "quantity int, unit_price decimal(12,2), loaded_at timestamp",
        )
        payments = self.spark.createDataFrame(
            [
                ("pay1", "o1", "completed", timestamp),
                ("pay2", "o1", "failed", timestamp),
            ],
            "payment_id string, order_id string, payment_status string, "
            "loaded_at timestamp",
        )

        sales = transform_sales(orders, items, payments)
        metrics = validate_sales(sales)
        rows = sales.orderBy("order_item_id").collect()

        self.assertEqual(metrics["row_count"], 2)
        self.assertEqual(metrics["distinct_order_item_count"], 2)
        self.assertEqual({row.payment_status for row in rows}, {"partially_paid"})
        self.assertEqual(rows[1].sales_amount, Decimal("40.00"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
