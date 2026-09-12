from datetime import datetime, timedelta
import os
import subprocess

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, dag, task


POSTGRES_CONN_ID = "ecommerce_postgres"
DBT_EXECUTABLE = "/opt/dbt-venv/bin/dbt"
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt")
DBT_TARGET = os.getenv("DBT_TARGET", "dev")
PIPELINE_NAMES = {
    "staging_customers",
    "staging_products",
    "staging_orders",
    "staging_order_items",
    "staging_payments",
}


def extract_incremental(
    table_name,
    primary_key,
    target_columns,
    source_columns=None,
    run_mode="incremental",
    backfill_start=None,
    backfill_end=None,
):
    """Upsert one closed source window and prepare its incremental watermark."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    pipeline_name = f"staging_{table_name}"

    watermark_row = hook.get_first(
        """
        SELECT watermark_value
        FROM metadata.etl_watermark
        WHERE pipeline_name = %s;
        """,
        parameters=(pipeline_name,),
    )
    if watermark_row is None:
        raise ValueError(f"Missing watermark for pipeline: {pipeline_name}")

    if run_mode == "backfill":
        lower_bound = backfill_start
        upper_bound = backfill_end
        lower_operator = ">="
        upper_operator = "<"
    else:
        lower_bound = watermark_row[0]
        upper_bound = hook.get_first(
            f"SELECT COALESCE(MAX(updated_at), %s) FROM public.{table_name};",
            parameters=(lower_bound,),
        )[0]
        lower_operator = ">"
        upper_operator = "<="

    source_columns = source_columns or target_columns
    target_column_list = ", ".join(target_columns)
    source_column_list = ", ".join(source_columns)
    update_columns = [
        column for column in target_columns if column != primary_key
    ]
    update_clause = ",\n".join(
        f"{column} = EXCLUDED.{column}" for column in update_columns
    )

    hook.run(
        f"""
        WITH staged_batch AS (
            INSERT INTO staging.{table_name} ({target_column_list})
            SELECT {source_column_list}
            FROM public.{table_name}
            WHERE updated_at {lower_operator} %s
              AND updated_at {upper_operator} %s
            ON CONFLICT ({primary_key})
            DO UPDATE SET
                {update_clause},
                loaded_at = CURRENT_TIMESTAMP
            RETURNING {primary_key}
        )

        UPDATE metadata.etl_watermark
        SET candidate_value = GREATEST(
            COALESCE(candidate_value, %s),
            %s
        )
        WHERE pipeline_name = %s
          AND %s = 'incremental';
        """,
        parameters=(
            lower_bound,
            upper_bound,
            upper_bound,
            upper_bound,
            pipeline_name,
            run_mode,
        ),
    )

@dag(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    params={
        "run_mode": Param(
            "incremental",
            enum=["incremental", "backfill"],
        ),
        "backfill_start": Param(
            None,
            type=["null", "string"],
            format="date-time",
        ),
        "backfill_end": Param(
            None,
            type=["null", "string"],
            format="date-time",
        ),
    },
    tags=["ecommerce", "production"]
)

def ecommerce_pipeline():
    @task
    def validate_run_config(**context):
        params = context["params"]
        if params["run_mode"] != "backfill":
            return

        start = params["backfill_start"]
        end = params["backfill_end"]
        if start is None or end is None:
            raise ValueError(
                "backfill_start and backfill_end are required in backfill mode"
            )

        start_at = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_at = datetime.fromisoformat(end.replace("Z", "+00:00"))
        if start_at >= end_at:
            raise ValueError("backfill_start must be earlier than backfill_end")

    @task(retries=2)
    def extract_customers(**context):
        params = context["params"]
        return extract_incremental(
            "customers",
            "customer_id",
            [
                "customer_id", "first_name", "last_name", "email",
                "created_at", "updated_at",
            ],
            run_mode=params["run_mode"],
            backfill_start=params["backfill_start"],
            backfill_end=params["backfill_end"],
        )

    @task(retries=2)
    def extract_products(**context):
        params = context["params"]
        return extract_incremental(
            "products",
            "product_id",
            [
                "product_id", "name", "category", "price",
                "created_at", "updated_at",
            ],
            run_mode=params["run_mode"],
            backfill_start=params["backfill_start"],
            backfill_end=params["backfill_end"],
        )

    @task(retries=2)
    def extract_orders(**context):
        params = context["params"]
        return extract_incremental(
            "orders",
            "order_id",
            [
                "order_id", "customer_id", "order_date", "status",
                "total_amount", "created_at", "updated_at",
            ],
            run_mode=params["run_mode"],
            backfill_start=params["backfill_start"],
            backfill_end=params["backfill_end"],
        )
    
    @task(retries=2)
    def extract_order_items(**context):
        params = context["params"]
        return extract_incremental(
            "order_items",
            "order_item_id",
            [
                "order_item_id", "order_id", "product_id", "quantity",
                "unit_price", "created_at", "updated_at",
            ],
            run_mode=params["run_mode"],
            backfill_start=params["backfill_start"],
            backfill_end=params["backfill_end"],
        )
    
    @task(retries=2)
    def extract_payments(**context):
        params = context["params"]
        return extract_incremental(
            "payments",
            "payment_id",
            [
                "payment_id", "order_id", "amount", "payment_method",
                "payment_status",
                "paid_at", "created_at", "updated_at",
            ],
            [
                "payment_id", "order_id", "amount", "payment_method",
                "status", "paid_at", "created_at", "updated_at",
            ],
            run_mode=params["run_mode"],
            backfill_start=params["backfill_start"],
            backfill_end=params["backfill_end"],
        )

    @task(retries=2)
    def validate_record_count():
        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )
        query = """
            SELECT
                (SELECT COUNT(*) FROM staging.customers) AS customers,
                (SELECT COUNT(*) FROM staging.products) AS products,
                (SELECT COUNT(*) FROM staging.orders) AS orders,
                (SELECT COUNT(*) FROM staging.order_items) AS order_items,
                (SELECT COUNT(*) FROM staging.payments) AS payments;
        """
        row = hook.get_first(query)
        counts = {
            "customers": row[0],
            "products": row[1],
            "orders": row[2],
            "order_items": row[3],
            "payments": row[4],
        }
        failures = [
            f"staging.{table}: row count = {count}"
            for table, count in counts.items()
            if count == 0
        ]

        if failures:
            raise ValueError(
                "Data validation failed:\n"
                + "\n".join(f"  - {failure}" for failure in failures)
            )
        print("Data validation passed:")
        for table, count in counts.items():
            print(f"  - staging.{table}: {count:,} rows")

    @task(retries=2)
    def validate_relationships():
        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )
        checks = {
            "orders_without_customer": """
                SELECT COUNT(*)
                FROM staging.orders o
                LEFT JOIN staging.customers c
                    ON c.customer_id = o.customer_id
                WHERE c.customer_id IS NULL
            """,

            "order_items_without_order": """
                SELECT COUNT(*)
                FROM staging.order_items oi
                LEFT JOIN staging.orders o
                    ON o.order_id = oi.order_id
                WHERE o.order_id IS NULL
            """,

            "order_items_without_product": """
                SELECT COUNT(*)
                FROM staging.order_items oi
                LEFT JOIN staging.products p
                    ON p.product_id = oi.product_id
                WHERE p.product_id IS NULL
            """,

            "payments_without_order": """
                SELECT COUNT(*)
                FROM staging.payments p
                LEFT JOIN staging.orders o
                    ON o.order_id = p.order_id
                WHERE o.order_id IS NULL
            """,
        }

        failures = []

        for check_name, query in checks.items():

            count = hook.get_first(query)[0]

            print(f"{check_name}: {count}")

            if count > 0:
                failures.append(
                    f"{check_name}: {count} invalid rows"
                )

        if failures:
            raise ValueError(
                "Relationship validation failed:\n"
                + "\n".join(
                    f"  - {failure}"
                    for failure in failures
                )
            )

    @task(
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=1)
    )
    def run_dbt_transform():
        """Build and test the complete dbt project"""
        command = [DBT_EXECUTABLE,
                "build",
                "--project-dir",
                DBT_PROJECT_DIR,
                "--profiles-dir",
                DBT_PROFILES_DIR,
                "--target",
                DBT_TARGET,
                "--no-partial-parse"
        ]
        print("Running dbt command: ", " ".join(command))
        subprocess.run(command, check=True)

    @task(retries=2)
    def advance_watermarks(**context):
        """Commit source windows only after all warehouse loads succeed."""
        if context["params"]["run_mode"] == "backfill":
            print("Backfill completed; incremental watermarks remain unchanged")
            return

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        candidate_rows = hook.get_records(
            """
            SELECT pipeline_name
            FROM metadata.etl_watermark
            WHERE pipeline_name = ANY(%s)
              AND candidate_value IS NOT NULL;
            """,
            parameters=(list(PIPELINE_NAMES),),
        )
        candidates = {row[0] for row in candidate_rows}
        missing_candidates = PIPELINE_NAMES - candidates

        if missing_candidates:
            raise ValueError(
                "Cannot advance incomplete watermark batch: "
                + ", ".join(sorted(missing_candidates))
            )

        hook.run(
            """
            UPDATE metadata.etl_watermark
            SET
                watermark_value = GREATEST(
                    watermark_value,
                    candidate_value
                ),
                candidate_value = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE pipeline_name = ANY(%s)
              AND candidate_value IS NOT NULL;
            """,
            parameters=(list(PIPELINE_NAMES),),
        )
        
    run_config = validate_run_config()
    customers = extract_customers()
    products = extract_products()
    orders = extract_orders()
    order_items = extract_order_items()
    payments = extract_payments()

    validated_record_count = validate_record_count()
    validated_relationships = validate_relationships()

    dbt_transform = run_dbt_transform()
    watermarks = advance_watermarks()

    run_config >> [
        customers,
        products,
        orders,
        order_items,
        payments,
    ] >> validated_record_count >> validated_relationships >> dbt_transform >> watermarks

ecommerce_pipeline()
