import random
from datetime import datetime, timedelta
from faker import Faker
fake = Faker()

TOTAL_ORDERS = 500000
BATCH_SIZE_ORDERS = 20000
ORDER_STATUS = [
    'pending',
    'confirmed',
    'processing',
    'shipped',
    'delivered',
    'canceled'
]

def load_customer_ids(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT customer_id FROM customers
        """)
        customer_ids = [row[0] for row in cur.fetchall()]
        return customer_ids

def generate_orders(conn, customer_ids):
    # customer_ids = get_customer_ids(conn)
    start_date = datetime.now() - timedelta(days=730)
    total_inserted = 0
    while total_inserted < TOTAL_ORDERS:
        current_batch_size = min(BATCH_SIZE_ORDERS, TOTAL_ORDERS - total_inserted)
        with conn.cursor() as cur:
            with cur.copy("""
                COPY orders (
                    customer_id,
                    ordered_at,
                    status,
                    total_amount
                )
                FROM STDIN
            """) as copy:
                for _ in range(current_batch_size):
                    customer_id = random.choice(customer_ids)
                    order_date = fake.date_between(start_date=start_date, end_date="now")
                    status = random.choice(ORDER_STATUS)
                    order_amount = round(random.uniform(10, 5000), 2)
                    copy.write_row(
                        (customer_id, order_date, status, order_amount)
                    )
        total_inserted +=  current_batch_size
        print(f"{total_inserted}/{TOTAL_ORDERS} ORDERS INSERTED")