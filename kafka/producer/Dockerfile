FROM python:3.12-slim

WORKDIR /app

# A dedicated image keeps the producer independently runnable.  The same
# requirements are also installed into Airflow's project venv for DAG execution.
COPY kafka/producer/requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir --requirement /app/requirements.txt

COPY kafka/producer/publish_orders.py /app/publish_orders.py
# The same small runtime is reused by finite CDC administration and smoke-test
# jobs. They need the same PostgreSQL and Kafka clients as the batch producer,
# while the long-running Debezium connector remains a separate Java process.
COPY cdc /app/cdc

# The producer only needs network access; UID nobody prevents accidental writes
# to image-owned paths and avoids running application code as root.
USER 65534

ENTRYPOINT ["python", "/app/publish_orders.py"]
