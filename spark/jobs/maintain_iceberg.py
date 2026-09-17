"""Compact Iceberg data files and expire old snapshots with a safety floor."""

from __future__ import annotations

import json

from iceberg_common import iceberg_spark


TABLE_NAME = "ecommerce.fact_sales"


def main() -> None:
    spark = iceberg_spark("ecommerce-iceberg-maintenance")
    spark.sparkContext.setLogLevel("WARN")
    try:
        rewrite_result = spark.sql(
            f"""
            CALL local.system.rewrite_data_files(
                table => '{TABLE_NAME}',
                options => map(
                    'target-file-size-bytes', '134217728',
                    'min-input-files', '5'
                )
            )
            """
        ).first()
        expire_result = spark.sql(
            f"""
            CALL local.system.expire_snapshots(
                table => '{TABLE_NAME}',
                older_than => CURRENT_TIMESTAMP - INTERVAL 7 DAYS,
                retain_last => 10
            )
            """
        ).first()
        print(
            "ICEBERG_MAINTENANCE_METRICS="
            + json.dumps(
                {
                    "rewrite": rewrite_result.asDict() if rewrite_result else {},
                    "expire": expire_result.asDict() if expire_result else {},
                },
                default=str,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
