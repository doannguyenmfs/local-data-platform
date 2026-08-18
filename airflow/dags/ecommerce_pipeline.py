from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime

POSTGRES_CONN_ID="ecommerce_postgres"

@dag(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "production"]
)

def ecommerce_pipeline():
    @task
    def extract_customers():
        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )
        hook.run("""
            TRUNCATE staging.customers;

            INSERT INTO staging.customers
            SELECT
                customer_id,
                first_name,
                last_name,
                email,
                created_at
            FROM public.customers;
        """)

    @task
    def extract_products():
        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )
        hook.run("""
            TRUNCATE staging.products;

            INSERT INTO staging.products
            SELECT
                product_id,
                name,
                category,
                price,
                created_at
            FROM public.products;
        """)

    @task
    def extract_orders():
        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )
        hook.run("""
            TRUNCATE staging.orders;

            INSERT INTO staging.orders
            SELECT
                order_id,
                customer_id,
                ordered_at,
                status,
                total_amount
            FROM public.orders;
        """)
    
    @task
    def extract_order_items():
        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )
        hook.run("""
            TRUNCATE staging.order_items;

            INSERT INTO staging.order_items
            SELECT
                order_item_id,
                order_id,
                product_id,
                quantity,
                unit_price
            FROM public.order_items;
        """)
    
    @task
    def extract_payments():
        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )
        hook.run("""
            TRUNCATE staging.payments;
            
            INSERT INTO staging.payments
            SELECT
                payment_id,
                order_id,
                amount,
                payment_method,
                status,
                paid_at
            FROM public.payments;
        """)

    @task
    def validate():
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

    @task
    def transform():
        pass

    @task
    def load():
        pass


    customers = extract_customers()
    products = extract_products()
    orders = extract_orders()
    order_items = extract_order_items()
    payments = extract_payments()

    validated = validate()
    validated_relationships = validate_relationships()

    transformed = transform()

    loaded = load()

    [
        customers,
        products,
        orders,
        order_items,
        payments,
    ] >> validated >> validated_relationships >> transformed >> loaded

ecommerce_pipeline()