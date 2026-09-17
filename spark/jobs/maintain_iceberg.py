"""Compact Iceberg data files and expire old snapshots with a safety floor."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from iceberg_common import iceberg_spark


TABLE_NAMES = (
    "ecommerce.fact_sales",
    "ecommerce.order_events",
    "ecommerce.current_orders",
    "ecommerce.dead_letter_events",
)


def main() -> None:
    spark = iceberg_spark("ecommerce-iceberg-maintenance")
    spark.sparkContext.setLogLevel("WARN")
    try:
        snapshot_cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        snapshot_cutoff_sql = snapshot_cutoff.strftime("%Y-%m-%d %H:%M:%S.%f")
        existing_tables = {
            f"ecommerce.{row.tableName}"
            for row in spark.sql("SHOW TABLES IN local.ecommerce").collect()
        }
        results = {}
        for table_name in TABLE_NAMES:
            if table_name not in existing_tables:
                results[table_name] = {"status": "not_created_yet"}
                continue

            rewrite_result = spark.sql(
                f"""
                CALL local.system.rewrite_data_files(
                    table => '{table_name}',
                    options => map(
                        'target-file-size-bytes', '134217728',
                        'min-input-files', '5'
                    )
                )
                """
            ).first()
            delete_result = None
            if table_name == "ecommerce.current_orders":
                delete_result = spark.sql(
                    f"""
                    CALL local.system.rewrite_position_delete_files(
                        table => '{table_name}'
                    )
                    """
                ).first()
            expire_result = spark.sql(
                f"""
                CALL local.system.expire_snapshots(
                    table => '{table_name}',
                    older_than => TIMESTAMP '{snapshot_cutoff_sql}',
                    retain_last => 10
                )
                """
            ).first()
            results[table_name] = {
                "rewrite_data_files": (
                    rewrite_result.asDict() if rewrite_result else {}
                ),
                "rewrite_position_delete_files": (
                    delete_result.asDict() if delete_result else {}
                ),
                "expire_snapshots": (
                    expire_result.asDict() if expire_result else {}
                ),
            }
        print(
            "ICEBERG_MAINTENANCE_METRICS="
            + json.dumps(results, default=str, sort_keys=True)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
