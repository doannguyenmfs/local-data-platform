"""Maintain all project-owned Iceberg tables outside the ingestion SLA.

The job compacts small data files, compacts merge-on-read position deletes for
the current-state table and expires old snapshots while retaining a minimum of
ten.  Missing stream-created tables are normal on a fresh deployment and are
reported rather than treated as a global failure.
"""

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
        # Compute the cutoff in UTC in Python and bind it as a literal accepted
        # consistently by the Iceberg Spark procedure parser.
        snapshot_cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        snapshot_cutoff_sql = snapshot_cutoff.strftime("%Y-%m-%d %H:%M:%S.%f")
        # Discover once so each optional table does not need an exception-based
        # existence probe.
        existing_tables = {
            f"ecommerce.{row.tableName}"
            for row in spark.sql("SHOW TABLES IN local.ecommerce").collect()
        }
        results = {}
        for table_name in TABLE_NAMES:
            if table_name not in existing_tables:
                results[table_name] = {"status": "not_created_yet"}
                continue

            # 128 MiB is a local target large enough to reduce metadata/file
            # overhead without requiring production-scale input volume.
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
            # current_orders uses merge-on-read; row updates produce position
            # delete files that need their own compaction procedure.
            if table_name == "ecommerce.current_orders":
                delete_result = spark.sql(
                    f"""
                    CALL local.system.rewrite_position_delete_files(
                        table => '{table_name}'
                    )
                    """
                ).first()
            # Both conditions matter: age controls retention, retain_last keeps
            # a recovery floor even when many snapshots are older than 7 days.
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
