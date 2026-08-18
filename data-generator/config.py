import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG= {
    "host": os.getenv("ECOMMERCE_POSTGRES_HOST"),
    "port": os.getenv("ECOMMERCE_POSTGRES_PORT"),
    "dbname": os.getenv("ECOMMERCE_POSTGRES_DB"),
    "user": os.getenv("ECOMMERCE_POSTGRES_USER"),
    "password": os.getenv("ECOMMERCE_POSTGRES_PASSWORD")
}