"""Weekly Iceberg maintenance, intentionally separate from ingest SLAs.

Compaction and snapshot expiration are housekeeping workloads: they consume
Spark resources but do not produce the daily business batch.  A dedicated DAG
gives them an independent schedule/retry history and keeps an operational
maintenance failure off the ingestion critical path.
"""

from datetime import datetime, timedelta
import os

import requests
from airflow.sdk import dag, task


SPARK_GATEWAY_URL = os.getenv("SPARK_GATEWAY_URL", "http://spark-gateway:8090")


@dag(
    dag_id="lakehouse_maintenance",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "iceberg", "maintenance"],
)
def lakehouse_maintenance():
    @task(
        retries=2,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(hours=2),
    )
    def compact_and_expire_snapshots():
        """Run the single allow-listed maintenance job and surface driver logs."""
        # Airflow never receives arbitrary Spark command text.  The gateway
        # maps this fixed job name to a version-controlled script.
        response = requests.post(
            f"{SPARK_GATEWAY_URL}/jobs/iceberg-maintenance",
            timeout=2 * 60 * 60,
        )
        if not response.ok:
            raise RuntimeError(
                f"Iceberg maintenance failed with HTTP {response.status_code}: "
                f"{response.text[-4000:]}"
            )
        result = response.json()
        print(result.get("log_tail", ""))
        if result.get("return_code") != 0:
            raise RuntimeError(f"Iceberg maintenance failed: {result}")

    compact_and_expire_snapshots()


lakehouse_maintenance()
