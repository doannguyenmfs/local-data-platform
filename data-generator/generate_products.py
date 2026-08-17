from faker import Faker
import random

TOTAL_PRODUCTS = 10000
CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home",
    "Beauty",
    "Sports",
    "Books",
    "Toys",
    "Food",
]

fake = Faker()

def generate_products(conn):
    with conn.cursor() as cur:
        with cur.copy(
            """
            COPY products (
                name,
                category,
                price,
                stock_quantity,
                is_active
            )
            FROM STDIN
            """
        ) as copy:
            for i in range(TOTAL_PRODUCTS):
                name = fake.catch_phrase()

                category = random.choice(CATEGORIES)

                price = round(
                    random.uniform(5, 2000),
                    2,
                )

                stock_quantity = random.randint(
                    0,
                    1000,
                )

                is_active = random.random() < 0.95

                copy.write_row(
                    (
                        name,
                        category,
                        price,
                        stock_quantity,
                        is_active,
                    )
                )

                if (i + 1) % 1_000 == 0:
                    print(
                        f"{i + 1:,}/{TOTAL_PRODUCTS:,} "
                        "PRODUCTS INSERTED"
                    )
