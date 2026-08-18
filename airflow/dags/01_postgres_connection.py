from datetime import datetime
from airflow.sdk import DAG, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id="postgres_connection",
    start_date=datetime(2026,8,1),
    schedule=None,
    catchup=False,
    tags=["postgres connect"]
):
    @task
    def inspect_data():
        hook = PostgresHook(
            postgres_conn_id="ecommerce_postgres"
        )
        rows = hook.get_records(
            """
                SELECT
                    order_date::date AS order_date,
                    COUNT(*) AS order_count
                FROM orders
                GROUP BY order_date::date
                ORDER BY order_date::date
                LIMIT 10
            """
        )
        for row in rows:
            print(row)

    inspect_data()