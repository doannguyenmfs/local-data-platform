"""Read-only post-deployment validation for the project-owned Iceberg tables.

This is intentionally an allow-listed gateway job rather than ad-hoc SQL from
Airflow.  It uses the exact runtime/catalog configuration as writers and fails
when a persisted table no longer has one row per declared business key.
"""

from __future__ import annotations

import json

from iceberg_common import iceberg_spark


TABLE_KEYS = {
    "fact_sales": "order_item_id",
    "order_events": "event_id",
    "current_orders": "order_id",
}


def main() -> None:
    spark = iceberg_spark("ecommerce-platform-validation")
    spark.sparkContext.setLogLevel("WARN")
    try:
        metrics: dict[str, dict[str, int]] = {}
        # Table/key names are repository constants, not request input, so SQL
        # identifier interpolation cannot be used to execute arbitrary SQL.
        for table_name, key_name in TABLE_KEYS.items():
            row = spark.sql(
                f"""
                SELECT
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT {key_name}) AS distinct_key_count
                FROM local.ecommerce.{table_name}
                """
            ).first()
            table_metrics = {
                "row_count": int(row.row_count),
                "distinct_key_count": int(row.distinct_key_count),
            }
            if table_metrics["row_count"] != table_metrics["distinct_key_count"]:
                raise ValueError(
                    f"{table_name} violates its declared {key_name} grain: "
                    f"{table_metrics}"
                )
            metrics[table_name] = table_metrics

        # DLQ is allowed to contain multiple rows and has a different operational
        # meaning; expose its count for triage instead of requiring zero here.
        dead_letters = spark.sql(
            "SELECT COUNT(*) AS row_count "
            "FROM local.ecommerce.dead_letter_events"
        ).first()
        metrics["dead_letter_events"] = {
            "row_count": int(dead_letters.row_count)
        }
        print("PLATFORM_VALIDATION_METRICS=" + json.dumps(metrics, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
