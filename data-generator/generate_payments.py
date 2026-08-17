import random
from datetime import timedelta

import psycopg

from config import DB_CONFIG


BATCH_SIZE = 10_000

PAYMENT_METHODS = [
    "credit_card",
    "cash",
    "debit_card",
    "bank_transfer",
    "e_wallet",
]


def load_orders(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                order_id,
                total_amount,
                ordered_at
            FROM orders;
            """
        )

        return cur.fetchall()


def generate_payments(conn, orders):
    total_orders = len(orders)
    processed_orders = 0
    total_payments = 0

    while processed_orders < total_orders:
        current_batch_size = min(
            BATCH_SIZE,
            total_orders - processed_orders,
        )

        with conn.cursor() as cur:
            with cur.copy(
                """
                COPY payments (
                    order_id,
                    amount,
                    payment_method,
                    status,
                    transaction_id,
                    paid_at
                )
                FROM STDIN
                """
            ) as copy:

                for i in range(current_batch_size):
                    order_id, order_amount, ordered_at = orders[
                        processed_orders + i
                    ]

                    # 90%: 1 payment
                    # 10%: 2-3 payment attempts
                    if random.random() < 0.90:
                        payment_count = 1
                    else:
                        payment_count = random.randint(2, 3)

                    for attempt in range(payment_count):
                        is_last_attempt = (
                            attempt == payment_count - 1
                        )

                        # Một payment duy nhất:
                        # đa số thành công, một phần nhỏ thất bại
                        if payment_count == 1:
                            status = random.choices(
                                ["completed", "failed"],
                                weights=[95, 5],
                                k=1,
                            )[0]

                        # Nếu có nhiều attempts:
                        # các attempt trước luôn failed
                        elif not is_last_attempt:
                            status = "failed"

                        # Attempt cuối có xác suất thành công cao
                        else:
                            status = random.choices(
                                ["completed", "failed"],
                                weights=[90, 10],
                                k=1,
                            )[0]

                        payment_method = random.choice(
                            PAYMENT_METHODS
                        )

                        transaction_id = None
                        paid_at = None

                        if status == "completed":
                            transaction_id = (
                                f"TXN-{order_id}-{attempt + 1}"
                            )

                            paid_at = (
                                ordered_at
                                + timedelta(
                                    minutes=random.randint(1, 60)
                                )
                            )

                        copy.write_row(
                            (
                                order_id,
                                order_amount,
                                payment_method,
                                status,
                                transaction_id,
                                paid_at,
                            )
                        )

                        total_payments += 1

        processed_orders += current_batch_size

        print(
            f"{processed_orders:,}/{total_orders:,} "
            f"ORDERS PROCESSED | "
            f"{total_payments:,} PAYMENTS GENERATED"
        )

