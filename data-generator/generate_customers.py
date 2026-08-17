from faker import Faker

TOTAL_CUSTOMERS = 100000
BATCH_SIZE_CUSTOMER = 10000

fake = Faker()

def generate_customers(conn):
    total_inserted = 0
    while (total_inserted < TOTAL_CUSTOMERS):
        current_batch_size = min(BATCH_SIZE_CUSTOMER, TOTAL_CUSTOMERS - total_inserted)
        with conn.cursor() as cur:
            with cur.copy(
                """
                    COPY customers (first_name, last_name, email)
                    FROM STDIN
                """
            ) as copy:
                for _ in range(current_batch_size):
                    first_name = fake.first_name()
                    last_name = fake.last_name()
                    email = fake.unique.email()
                    copy.write_row((first_name, last_name, email))
        total_inserted += current_batch_size
        print(f"{total_inserted}/{TOTAL_CUSTOMERS} CUSTOMERS INSERTED")