import psycopg
from faker import Faker
from config import DB_CONFIG
import random

from generate_customers import generate_customers
from generate_products import generate_products
from generate_orders import generate_orders, load_customer_ids
from generate_order_items import load_ids, generate_order_items
from generate_payments import load_orders, generate_payments
def main():
    with psycopg.connect(**DB_CONFIG) as conn:
        # generate_products(conn)
        # generate_customers(conn)

        # customer_ids = load_customer_ids(conn)
        # print(f"Loaded {len(customer_ids):,} customers)
        # generate_orders(conn, customer_ids)

        # order_ids, product_ids = load_ids(conn)
        # print(f"Loaded {len(order_ids):,} orders")
        # print(f"Loaded {len(product_ids):,} products")
        # generate_order_items(
        #     conn,
        #     order_ids,
        #     product_ids,
        # )

        orders = load_orders(conn)
        print(
            f"Loaded {len(orders):,} orders"
        )
        generate_payments(conn, orders)

if __name__=="__main__":
    main()