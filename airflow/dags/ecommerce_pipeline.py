from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime

POSTGRES_CONN_ID="ecommerce_postgres"
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
    schedule=None,
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
    @task(retries=2)
    def load_dim_customer():
        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        hook.run("""
            -- 1. Close current records that have changed
            UPDATE analytics.dim_customer AS dim
            SET
                valid_to = CURRENT_TIMESTAMP,
                is_current = FALSE
            FROM staging.customers AS src
            WHERE dim.customer_id = src.customer_id
            AND dim.is_current = TRUE
            AND dim.record_hash <> md5(
                    concat_ws(
                        '||',
                        src.first_name,
                        src.last_name,
                        src.email
                    )
            );

            -- 2. Insert new customers and new versions
            INSERT INTO analytics.dim_customer (
                customer_id,
                first_name,
                last_name,
                email,
                record_hash,
                valid_from,
                valid_to,
                is_current,
                created_at
            )
            SELECT
                src.customer_id,
                src.first_name,
                src.last_name,
                src.email,
                md5(
                    concat_ws(
                        '||',
                        src.first_name,
                        src.last_name,
                        src.email
                    )
                ) AS record_hash,
                CURRENT_TIMESTAMP AS valid_from,
                NULL AS valid_to,
                TRUE AS is_current,
                CURRENT_TIMESTAMP AS created_at
            FROM staging.customers AS src
            LEFT JOIN analytics.dim_customer AS dim
                ON dim.customer_id = src.customer_id
            AND dim.is_current = TRUE
            WHERE dim.customer_id IS NULL;
            """
        )

    @task(retries=2)
    def load_dim_product():
        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )
        hook.run(
            """
            INSERT INTO analytics.dim_product (
                product_id,
                name,
                category,
                price,
                created_at
            )
            SELECT
                product_id,
                name,
                category,
                price,
                created_at
            FROM staging.products

            ON CONFLICT (product_id)
            DO UPDATE SET
                name = EXCLUDED.name,
                category = EXCLUDED.category,
                price = EXCLUDED.price,
                created_at = EXCLUDED.created_at;
            """
        )

    @task(retries=2)
    def transform_fact_sales():
        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )
        hook.run(
            """
            INSERT INTO analytics.fact_sales (
                order_id,
                order_item_id,
                customer_id,
                customer_sk,
                product_id,
                order_date,
                quantity,
                unit_price,
                sales_amount,
                payment_status
            )
            SELECT
                o.order_id,
                oi.order_item_id,
                o.customer_id,
                dc.customer_sk,
                oi.product_id,
                o.order_date,
                oi.quantity,
                oi.unit_price,

                oi.quantity * oi.unit_price
                    AS sales_amount,

                COALESCE(
                    p.payment_status,
                    'unpaid'
                ) AS payment_status

            FROM staging.orders o

            JOIN staging.order_items oi
                ON oi.order_id = o.order_id

            JOIN analytics.dim_customer dc
                ON dc.customer_id = o.customer_id
                AND dc.is_current = TRUE

            JOIN staging.products pr
                ON pr.product_id = oi.product_id

            LEFT JOIN (
                SELECT
                    order_id,

                    CASE
                        WHEN BOOL_AND(
                            payment_status = 'completed'
                        )
                            THEN 'paid'

                        WHEN BOOL_OR(
                            payment_status = 'completed'
                        )
                            THEN 'partially_paid'

                        ELSE 'unpaid'
                    END AS payment_status

                FROM staging.payments

                GROUP BY order_id
            ) p
                ON p.order_id = o.order_id

            ON CONFLICT (
                order_id,
                order_item_id
            )
            DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                customer_sk = EXCLUDED.customer_sk,
                product_id = EXCLUDED.product_id,
                order_date = EXCLUDED.order_date,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                sales_amount = EXCLUDED.sales_amount,
                payment_status = EXCLUDED.payment_status;
            """
        )

    @task(retries=2)
    def load_daily_sales():
        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        hook.run(
            """
            INSERT INTO analytics.daily_sales (
                sales_date,
                total_orders,
                total_items,
                total_sales,
                paid_sales
            )
            SELECT
                order_date::date AS sales_date,

                COUNT(DISTINCT order_id)
                    AS total_orders,

                SUM(quantity)
                    AS total_items,

                SUM(sales_amount)
                    AS total_sales,

                SUM(
                    CASE
                        WHEN payment_status = 'paid'
                            THEN sales_amount
                        ELSE 0
                    END
                ) AS paid_sales

            FROM analytics.fact_sales

            GROUP BY order_date::date

            ON CONFLICT (sales_date)
            DO UPDATE SET
                total_orders = EXCLUDED.total_orders,
                total_items = EXCLUDED.total_items,
                total_sales = EXCLUDED.total_sales,
                paid_sales = EXCLUDED.paid_sales;
            """
        )

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

    dim_customer = load_dim_customer()
    dim_product = load_dim_product()
    fact_sale = transform_fact_sales()
    daily_sales = load_daily_sales()
    watermarks = advance_watermarks()

    run_config >> [
        customers,
        products,
        orders,
        order_items,
        payments,
    ] >> validated_record_count >> validated_relationships >> [dim_customer, dim_product]
    dim_customer >> fact_sale >> daily_sales
    [daily_sales, dim_product] >> watermarks

ecommerce_pipeline()
