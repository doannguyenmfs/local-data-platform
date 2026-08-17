import random

BATCH_SIZE = 10_000


def load_ids(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT order_id FROM orders;")
        order_ids = [row[0] for row in cur.fetchall()]

        cur.execute("SELECT product_id FROM products;")
        product_ids = [row[0] for row in cur.fetchall()]

    return order_ids, product_ids


def generate_order_items(conn, order_ids, product_ids):
    total_orders = len(order_ids)
    processed_orders = 0
    total_items = 0

    while processed_orders < total_orders:
        current_batch_size = min(
            BATCH_SIZE,
            total_orders - processed_orders,
        )

        with conn.cursor() as cur:
            with cur.copy(
                """
                COPY order_items (
                    order_id,
                    product_id,
                    quantity,
                    unit_price
                )
                FROM STDIN
                """
            ) as copy:

                for i in range(current_batch_size):
                    order_id = order_ids[
                        processed_orders + i
                    ]

                    item_count = random.randint(1, 5)

                    selected_products = random.sample(
                        product_ids,
                        item_count,
                    )

                    for product_id in selected_products:
                        quantity = random.randint(1, 5)

                        unit_price = round(
                            random.uniform(5, 2000),
                            2,
                        )

                        copy.write_row(
                            (
                                order_id,
                                product_id,
                                quantity,
                                unit_price,
                            )
                        )

                        total_items += 1

        processed_orders += current_batch_size

        print(
            f"{processed_orders:,}/{total_orders:,} "
            f"ORDERS PROCESSED | "
            f"{total_items:,} ITEMS GENERATED"
        )
