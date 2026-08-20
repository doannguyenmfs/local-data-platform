from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

POSTGRES_CONN_ID="ecommerce_postgres"
SQL_FILES = [
    "/opt/postgres-sql-statement/init/002_staging_analytics_schema.sql",
    "/opt/postgres-sql-statement/init/003_create_etl_metadata.sql",
]

@dag(
    dag_id="eccomerce_reset_other_schema",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    tags=["reset"]
)
def eccomerce_reset_other_schema():
    @task()
    def reset():
        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )
        for sql_file in SQL_FILES:
            with open(sql_file, encoding='utf-8') as sql_content:
                hook.run(sql=sql_content.read())

    reset_schema = reset()
eccomerce_reset_other_schema()