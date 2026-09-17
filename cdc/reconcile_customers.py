"""Reconcile OLTP customer truth with the CDC Silver current-state view."""

from __future__ import annotations

import os

import psycopg


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def main() -> None:
    with psycopg.connect(
        host=required("ECOMMERCE_POSTGRES_HOST"),
        port=required("ECOMMERCE_POSTGRES_PORT"),
        user=required("ECOMMERCE_POSTGRES_USER"),
        password=required("ECOMMERCE_POSTGRES_PASSWORD"),
        dbname=required("ECOMMERCE_POSTGRES_DB"),
    ) as connection:
        with connection.cursor() as cursor:
            # FULL OUTER JOIN catches missing, unexpected and stale attribute
            # rows in one symmetric comparison. `IS DISTINCT FROM` is NULL-safe.
            cursor.execute(
                """
                WITH differences AS (
                    SELECT
                        COALESCE(source.customer_id, silver.customer_id) AS customer_id
                    FROM public.customers AS source
                    FULL OUTER JOIN cdc_silver.customers_current AS silver
                        USING (customer_id)
                    WHERE source.customer_id IS NULL
                       OR silver.customer_id IS NULL
                       OR source.first_name IS DISTINCT FROM silver.first_name
                       OR source.last_name IS DISTINCT FROM silver.last_name
                       OR source.email IS DISTINCT FROM silver.email
                       OR source.created_at IS DISTINCT FROM silver.created_at
                       OR source.updated_at IS DISTINCT FROM silver.updated_at
                )
                SELECT
                    (SELECT COUNT(*) FROM public.customers) AS source_count,
                    (SELECT COUNT(*) FROM cdc_silver.customers_current) AS silver_count,
                    (SELECT COUNT(*) FROM differences) AS difference_count,
                    (
                        SELECT COUNT(*)
                        FROM (
                            SELECT kafka_topic, kafka_partition, kafka_offset
                            FROM cdc_bronze.customer_changes
                            GROUP BY 1, 2, 3
                            HAVING COUNT(*) > 1
                        ) AS duplicate_offsets
                    ) AS duplicate_offset_count
                """
            )
            source_count, silver_count, differences, duplicate_offsets = (
                cursor.fetchone()
            )

    print(
        "Customer reconciliation: "
        f"source={source_count}, silver_current={silver_count}, "
        f"differences={differences}, duplicate_offsets={duplicate_offsets}"
    )
    if differences or duplicate_offsets:
        raise AssertionError("CDC customer reconciliation failed")


if __name__ == "__main__":
    main()
