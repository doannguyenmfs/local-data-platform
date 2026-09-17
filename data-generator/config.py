"""Load host-side generator connection settings from the local ``.env`` file.

The generator runs directly on the developer machine, so its host is usually
``localhost`` rather than the Compose service name ``postgres``.
"""

import os
from dotenv import load_dotenv

# Loading here keeps every generator module on one configuration contract.
load_dotenv()

DB_CONFIG= {
    "host": os.getenv("ECOMMERCE_POSTGRES_HOST"),
    "port": os.getenv("ECOMMERCE_POSTGRES_PORT"),
    "dbname": os.getenv("ECOMMERCE_POSTGRES_DB"),
    "user": os.getenv("ECOMMERCE_POSTGRES_USER"),
    "password": os.getenv("ECOMMERCE_POSTGRES_PASSWORD")
}
