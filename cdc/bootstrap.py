"""Prepare PostgreSQL CDC privileges and register the Debezium connector.

This is a finite, idempotent deployment job.  It deliberately separates DBA
work (role/publication) from the long-running Kafka Connect worker, then uses a
PUT request so rerunning Compose converges the connector configuration instead
of failing because the connector already exists.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

import psycopg
from psycopg import sql


CAPTURED_TABLES = ("customers",)


def required(name: str) -> str:
    """Read required runtime configuration without logging secret values."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def configure_postgres() -> None:
    """Create a least-privilege replication user and an explicit publication."""
    admin_user = required("ECOMMERCE_POSTGRES_USER")
    cdc_user = required("CDC_POSTGRES_USER")
    cdc_password = required("CDC_POSTGRES_PASSWORD")
    database = required("ECOMMERCE_POSTGRES_DB")

    # Autocommit makes every administrative statement independently durable and
    # avoids wrapping publication DDL in a long-lived transaction.
    with psycopg.connect(
        host=required("ECOMMERCE_POSTGRES_HOST"),
        port=required("ECOMMERCE_POSTGRES_PORT"),
        user=admin_user,
        password=required("ECOMMERCE_POSTGRES_PASSWORD"),
        dbname=database,
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s",
                (cdc_user,),
            )
            if cursor.fetchone() is None:
                # PostgreSQL utility statements such as CREATE ROLE do not
                # accept extended-protocol bind placeholders for PASSWORD.
                # psycopg.sql.Literal still performs correct SQL quoting; do
                # not replace it with string interpolation.
                cursor.execute(
                    sql.SQL(
                        "CREATE ROLE {} WITH LOGIN REPLICATION PASSWORD {}"
                    ).format(
                        sql.Identifier(cdc_user),
                        sql.Literal(cdc_password),
                    )
                )
            else:
                # Password rotation and accidentally removed privileges are
                # repaired on every bootstrap without recreating the role.
                cursor.execute(
                    sql.SQL(
                        "ALTER ROLE {} WITH LOGIN REPLICATION PASSWORD {}"
                    ).format(
                        sql.Identifier(cdc_user),
                        sql.Literal(cdc_password),
                    )
                )

            cursor.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(database), sql.Identifier(cdc_user)
                )
            )
            cursor.execute(
                sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
                    sql.Identifier(cdc_user)
                )
            )
            cursor.execute(
                sql.SQL("GRANT SELECT ON {} TO {}").format(
                    sql.SQL(", ").join(
                        sql.Identifier("public", table)
                        for table in CAPTURED_TABLES
                    ),
                    sql.Identifier(cdc_user),
                )
            )

            publication = sql.Identifier(required("CDC_PUBLICATION_NAME"))
            cursor.execute(
                "SELECT 1 FROM pg_publication WHERE pubname = %s",
                (required("CDC_PUBLICATION_NAME"),),
            )
            table_list = sql.SQL(", ").join(
                sql.Identifier("public", table) for table in CAPTURED_TABLES
            )
            if cursor.fetchone() is None:
                cursor.execute(
                    sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(
                        publication, table_list
                    )
                )
            else:
                # SET TABLE removes accidentally captured relations and makes
                # CAPTURED_TABLES the source-controlled publication contract.
                cursor.execute(
                    sql.SQL("ALTER PUBLICATION {} SET TABLE {}").format(
                        publication, table_list
                    )
                )

    print(
        "PostgreSQL CDC role/publication ready for: "
        + ", ".join(f"public.{table}" for table in CAPTURED_TABLES)
    )


def request_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, object] | None = None,
) -> object:
    """Call Kafka Connect's REST API and return decoded JSON."""
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def wait_for_connect(base_url: str, timeout_seconds: int = 180) -> None:
    """Wait for the worker REST API and PostgreSQL plug-in to be ready."""
    deadline = time.monotonic() + timeout_seconds
    last_error = "not started"
    while time.monotonic() < deadline:
        try:
            plugins = request_json(f"{base_url}/connector-plugins")
            classes = {plugin["class"] for plugin in plugins}
            if "io.debezium.connector.postgresql.PostgresConnector" in classes:
                return
            last_error = "PostgreSQL connector plug-in not listed"
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
            last_error = f"{type(error).__name__}: {error}"
        time.sleep(2)
    raise TimeoutError(f"Kafka Connect did not become ready: {last_error}")


def connector_config() -> dict[str, str]:
    """Build the source-controlled connector contract from runtime secrets."""
    topic_prefix = required("CDC_TOPIC_PREFIX")
    registry_url = required("SCHEMA_REGISTRY_NATIVE_URL")

    # Connector-level converters make the data-topic wire contract explicit.
    # Kafka Connect's worker defaults can remain JSON for unrelated connectors.
    # `as-confluent` uses the widely supported magic-byte + schema-id wire
    # format, while Apicurio remains the registry implementation behind it.
    avro_converter = "io.apicurio.registry.utils.converter.AvroConverter"
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        # PostgreSQL WAL is one ordered stream per connector; tasks.max > 1
        # would not parallelize this connector.
        "tasks.max": "1",
        "database.hostname": required("ECOMMERCE_POSTGRES_HOST"),
        "database.port": required("ECOMMERCE_POSTGRES_PORT"),
        "database.user": required("CDC_POSTGRES_USER"),
        "database.password": required("CDC_POSTGRES_PASSWORD"),
        "database.dbname": required("ECOMMERCE_POSTGRES_DB"),
        "topic.prefix": topic_prefix,
        "plugin.name": "pgoutput",
        "slot.name": required("CDC_SLOT_NAME"),
        "slot.drop.on.stop": "false",
        "publication.name": required("CDC_PUBLICATION_NAME"),
        # A DBA-owned explicit publication avoids granting CREATE/ownership of
        # application tables to the runtime replication user.
        "publication.autocreate.mode": "disabled",
        "table.include.list": ",".join(
            f"public.{table}" for table in CAPTURED_TABLES
        ),
        # `initial` emits op=r for current rows once, records the WAL LSN, then
        # continues with committed c/u/d changes without a gap.
        "snapshot.mode": "initial",
        "tombstones.on.delete": "true",
        "provide.transaction.metadata": "true",
        "heartbeat.interval.ms": "10000",
        "schema.name.adjustment.mode": "avro",
        "key.converter": avro_converter,
        "key.converter.apicurio.registry.url": registry_url,
        "key.converter.apicurio.registry.auto-register": "true",
        "key.converter.apicurio.registry.find-latest": "true",
        "key.converter.apicurio.registry.as-confluent": "true",
        "key.converter.apicurio.use-id": "contentId",
        "key.converter.apicurio.registry.headers.enabled": "false",
        "key.converter.schemas.enable": "false",
        "value.converter": avro_converter,
        "value.converter.apicurio.registry.url": registry_url,
        "value.converter.apicurio.registry.auto-register": "true",
        "value.converter.apicurio.registry.find-latest": "true",
        "value.converter.apicurio.registry.as-confluent": "true",
        "value.converter.apicurio.use-id": "contentId",
        "value.converter.apicurio.registry.headers.enabled": "false",
        "value.converter.schemas.enable": "false",
        # Broker auto-create is disabled. Kafka Connect creates topics with an
        # explicit default, while table topics get log compaction for keyed
        # current-state consumers. Local RF=1 reflects the one-broker topology.
        "topic.creation.default.replication.factor": "1",
        "topic.creation.default.partitions": "1",
        "topic.creation.default.cleanup.policy": "delete",
        "topic.creation.default.retention.ms": "604800000",
        "topic.creation.groups": "tables",
        "topic.creation.tables.include": rf"{topic_prefix}\.public\..*",
        "topic.creation.tables.partitions": "3",
        "topic.creation.tables.cleanup.policy": "compact",
        "topic.creation.tables.delete.retention.ms": "86400000",
        "topic.creation.tables.compression.type": "lz4",
    }


def register_connector() -> None:
    """Upsert connector configuration and require connector/task RUNNING."""
    base_url = required("DEBEZIUM_CONNECT_URL").rstrip("/")
    connector_name = required("CDC_CONNECTOR_NAME")
    wait_for_connect(base_url)
    request_json(
        f"{base_url}/connectors/{connector_name}/config",
        method="PUT",
        payload=connector_config(),
    )

    deadline = time.monotonic() + 180
    latest_status: object = None
    while time.monotonic() < deadline:
        try:
            latest_status = request_json(
                f"{base_url}/connectors/{connector_name}/status"
            )
        except urllib.error.HTTPError as error:
            # PUT is accepted by the leader before the distributed config log
            # has necessarily propagated back to this REST worker. A transient
            # 404 is therefore startup convergence, not connector failure.
            if error.code == 404:
                latest_status = {"http_status": 404, "state": "propagating"}
                time.sleep(2)
                continue
            raise
        connector_running = (
            latest_status.get("connector", {}).get("state") == "RUNNING"
        )
        tasks = latest_status.get("tasks", [])
        tasks_running = bool(tasks) and all(
            task.get("state") == "RUNNING" for task in tasks
        )
        if connector_running and tasks_running:
            print(
                f"Debezium connector {connector_name!r} is RUNNING "
                f"with {len(tasks)} task"
            )
            return
        if any(task.get("state") == "FAILED" for task in tasks):
            break
        time.sleep(2)
    raise RuntimeError(
        "Debezium connector failed to reach RUNNING state: "
        + json.dumps(latest_status, indent=2)
    )


def main() -> None:
    configure_postgres()
    register_connector()


if __name__ == "__main__":
    main()
