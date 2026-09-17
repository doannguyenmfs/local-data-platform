"""Entry point that generates source tables in foreign-key-safe order.

This utility seeds OLTP-shaped data only.  Airflow/dbt/Spark later move and
transform it; the generator does not write staging or analytical schemas.
"""

import psycopg
from config import DB_CONFIG
from pathlib import Path

from generate_customers import generate_customers
from generate_products import generate_products
from generate_orders import generate_orders, load_customer_ids
from generate_order_items import load_ids, generate_order_items
from generate_payments import load_orders, generate_payments


def execute_sql_file(conn, filename: str) -> None:
    """Apply a trusted repository SQL correction inside the same connection."""
    sql_path = Path(__file__).parent.parent / "postgres" / "sql" / filename
    with conn.cursor() as cur:
        cur.execute(sql_path.read_text())
    print(f"Executed {sql_path.name}")


def main():
    """Generate parents before children so database constraints stay active."""
    with psycopg.connect(**DB_CONFIG) as conn:
        # product
        generate_products(conn)
        
        # customer
        generate_customers(conn)

        # order (1 customer - N order)
        customer_ids = load_customer_ids(conn)
        print(f"Loaded {len(customer_ids)} customers")
        generate_orders(conn, customer_ids)

        # order item (1 order - N item)
        order_ids, product_ids = load_ids(conn)
        print(f"Loaded {len(order_ids):,} orders")
        print(f"Loaded {len(product_ids):,} products")
        generate_order_items(
            conn,
            order_ids,
            product_ids,
        )

        # payment (1 order - N payment)
        orders = load_orders(conn)
        print(
            f"Loaded {len(orders):,} orders"
        )
        generate_payments(conn, orders)

        # Corrections create deterministic edge cases used by validation/model
        # lessons after the random bulk generation has completed.
        execute_sql_file(conn, "001_correction.sql")

if __name__=="__main__":
    main()
