"""Prove create/update/delete propagation through Bronze and Silver."""

from __future__ import annotations

import os
import time
import uuid

import psycopg


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=required("ECOMMERCE_POSTGRES_HOST"),
        port=required("ECOMMERCE_POSTGRES_PORT"),
        user=required("ECOMMERCE_POSTGRES_USER"),
        password=required("ECOMMERCE_POSTGRES_PASSWORD"),
        dbname=required("ECOMMERCE_POSTGRES_DB"),
    )


def wait_for(connection: psycopg.Connection, query: str, params: tuple) -> tuple:
    """Poll committed materialization state with a bounded timeout."""
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        connection.rollback()  # start a fresh READ COMMITTED transaction
        row = connection.execute(query, params).fetchone()
        if row and row[0]:
            return row
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for materialized state: {query}")


def main() -> None:
    customer_id = uuid.uuid4()
    email = f"cdc-materialized-{customer_id}@example.test"
    with connect() as connection:
        try:
            connection.execute(
                """
                INSERT INTO public.customers (
                    customer_id, first_name, last_name, email
                ) VALUES (%s, 'CDC', 'Created', %s)
                """,
                (customer_id, email),
            )
            connection.commit()
            wait_for(
                connection,
                """
                SELECT COUNT(*) = 1
                FROM cdc_silver.customers_current
                WHERE customer_id = %s AND last_name = 'Created'
                """,
                (customer_id,),
            )

            connection.execute(
                """
                UPDATE public.customers
                SET last_name = 'Updated', updated_at = clock_timestamp()
                WHERE customer_id = %s
                """,
                (customer_id,),
            )
            connection.commit()
            wait_for(
                connection,
                """
                SELECT COUNT(*) = 1
                FROM cdc_silver.customers_current
                WHERE customer_id = %s AND last_name = 'Updated'
                """,
                (customer_id,),
            )

            connection.execute(
                "DELETE FROM public.customers WHERE customer_id = %s",
                (customer_id,),
            )
            connection.commit()
            wait_for(
                connection,
                """
                SELECT
                    NOT EXISTS (
                        SELECT 1 FROM cdc_silver.customers_current
                        WHERE customer_id = %s
                    )
                    AND EXISTS (
                        SELECT 1 FROM cdc_silver.customers
                        WHERE customer_id = %s AND is_deleted
                    )
                    AND ARRAY['c', 'u', 'd', 't']::CHAR(1)[] <@ ARRAY(
                        SELECT operation
                        FROM cdc_bronze.customer_changes
                        WHERE customer_id = %s
                    )
                """,
                (customer_id, customer_id, customer_id),
            )
        finally:
            # The normal path already deleted the source row. This cleanup also
            # prevents a failed verification from polluting later tests.
            connection.rollback()
            connection.execute(
                "DELETE FROM public.customers WHERE customer_id = %s",
                (customer_id,),
            )
            connection.commit()

    print(
        "CDC materialization test passed: Bronze=c,u,d,t; "
        f"Silver delete applied; customer_id={customer_id}"
    )


if __name__ == "__main__":
    main()
