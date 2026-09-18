"""Allow-listed HTTP gateway for submitting PySpark jobs in client mode.

Spark standalone does not support Python applications in cluster deploy mode.
The gateway keeps the Python driver in a long-running container while executors
run on Spark workers. It also avoids mounting the Docker socket into Airflow.

This is a laptop-scale adapter, not a general remote shell or a production job
server. Callers select a reviewed capability name from ``JOBS``; they cannot
submit an arbitrary path/command. The single lock intentionally serializes
memory-heavy finite jobs on the one-worker P0 topology.
"""

from __future__ import annotations

import json
import subprocess
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


JOBS = {
    "iceberg-sales": ["/opt/spark/jobs/iceberg_sales.py"],
    "iceberg-maintenance": ["/opt/spark/jobs/maintain_iceberg.py"],
    "validate-platform": ["/opt/spark/jobs/validate_platform.py"],
}
# The mapping is the security boundary: requests choose a capability name, not
# a script path or arbitrary command.  Adding a job requires a code review.
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"
JOB_TIMEOUT_SECONDS = 60 * 60
job_lock = threading.Lock()


class JobGatewayHandler(BaseHTTPRequestHandler):
    """Minimal synchronous API used by Airflow and operator validation calls."""

    server_version = "LocalDataPlatformJobGateway/1.0"

    def send_json(self, status: HTTPStatus, payload: dict) -> None:
        """Write a complete JSON response with an explicit byte length."""
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        """Report process liveness and whether a finite job currently owns it."""
        if self.path != "/health":
            self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        self.send_json(HTTPStatus.OK, {"status": "ok", "busy": job_lock.locked()})

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        """Run one allow-listed job; reject concurrent submissions on this lab."""
        prefix = "/jobs/"
        if not self.path.startswith(prefix):
            self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        job_name = self.path[len(prefix) :]
        job_args = JOBS.get(job_name)
        if job_args is None:
            self.send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "unknown_job", "allowed_jobs": sorted(JOBS)},
            )
            return
        # A single local worker cannot safely host two memory-heavy finite jobs
        # plus the stream.  Return 409 so Airflow can retry instead of queueing
        # an unbounded number of driver subprocesses inside this container.
        if not job_lock.acquire(blocking=False):
            self.send_json(HTTPStatus.CONFLICT, {"error": "gateway_busy"})
            return

        # Client deploy mode keeps the Python driver here.  Spark standalone
        # does not support Python cluster deploy mode; executors still run on
        # spark-worker.  Explicit driver host/bind settings make it reachable
        # from the executor across the Compose network.
        command = [
            SPARK_SUBMIT,
            "--master",
            SPARK_MASTER,
            "--deploy-mode",
            "client",
            "--conf",
            "spark.driver.host=spark-gateway",
            "--conf",
            "spark.driver.bindAddress=0.0.0.0",
            "--conf",
            "spark.executor.cores=1",
            "--conf",
            "spark.cores.max=1",
            "--conf",
            "spark.executor.memory=2g",
            *job_args,
        ]
        try:
            # Merge stderr into stdout so Airflow receives one chronological
            # log tail.  The full Spark logs remain in container logs.
            result = subprocess.run(
                command,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=JOB_TIMEOUT_SECONDS,
            )
            payload = {
                "job": job_name,
                "return_code": result.returncode,
                "log_tail": result.stdout[-12000:],
            }
            status = HTTPStatus.OK if result.returncode == 0 else HTTPStatus.BAD_GATEWAY
            self.send_json(status, payload)
        except subprocess.TimeoutExpired as error:
            self.send_json(
                HTTPStatus.GATEWAY_TIMEOUT,
                {"job": job_name, "error": "job_timeout", "detail": str(error)},
            )
        finally:
            job_lock.release()

    def log_message(self, message_format: str, *args) -> None:
        print(f"gateway client={self.client_address[0]} " + message_format % args)


if __name__ == "__main__":
    # Threaded HTTP handling keeps /health responsive; job_lock still permits
    # only one POST job at a time.
    server = ThreadingHTTPServer(("0.0.0.0", 8090), JobGatewayHandler)
    print("Spark job gateway listening on 0.0.0.0:8090")
    server.serve_forever()
