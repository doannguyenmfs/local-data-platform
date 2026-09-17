"""Verify registry policy and the actual Debezium Avro subjects.

This is a read-only contract smoke test. It waits until Debezium has registered
the transport key/envelope plus the reusable customer-row schema. It then proves
that the key and row contracts are compatible with themselves and requires the
registry to reject an intentionally breaking field-type change.

The breaking schema is never registered. Using the compatibility endpoint makes
the test safe to repeat and keeps the registry history free of synthetic test
versions.
"""

from __future__ import annotations

import copy
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request


def required(name: str) -> str:
    """Read mandatory configuration without inventing environment defaults."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def request_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, object] | None = None,
) -> object:
    """Call the Confluent-compatible registry API with bounded I/O."""
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def wait_for_subjects(
    base_url: str,
    expected_subjects: set[str],
    timeout_seconds: int = 300,
) -> None:
    """Wait for initial-snapshot records to register key and value contracts."""
    deadline = time.monotonic() + timeout_seconds
    latest_subjects: set[str] = set()
    latest_error = "registry has not answered yet"
    while time.monotonic() < deadline:
        try:
            latest_subjects = set(request_json(f"{base_url}/subjects"))
            if expected_subjects <= latest_subjects:
                return
            latest_error = (
                "missing subjects: "
                + ", ".join(sorted(expected_subjects - latest_subjects))
            )
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
            latest_error = f"{type(error).__name__}: {error}"
        time.sleep(2)
    raise TimeoutError(f"Debezium schemas were not registered: {latest_error}")


def break_first_field_type(schema: dict[str, object]) -> dict[str, object]:
    """Return a copy with one existing field changed incompatibly.

    Debezium's top-level key/value schemas are Avro records. Changing an
    existing field from its declared type to bytes is intentionally unsafe for
    previously serialized records and should fail BACKWARD_TRANSITIVE checks.
    Avro permits promotion between string and bytes, so those two types are not
    a valid negative test.
    """
    broken = copy.deepcopy(schema)
    fields = broken.get("fields")
    if not isinstance(fields, list) or not fields:
        raise ValueError("Expected a non-empty Avro record schema")
    original_type = fields[0].get("type")
    if isinstance(original_type, list):
        # Debezium value envelopes start with an optional nested `before`
        # record. Replacing that record with an optional scalar is valid Avro
        # syntax but cannot read the old nested-record payload.
        fields[0]["type"] = ["null", "long"]
    else:
        # The customer key is a Connect UUID represented as an Avro string.
        # String -> long has no Avro promotion rule. Keep the proposed schema
        # syntactically valid by changing the old UUID-string default too;
        # otherwise the API would reject validity before testing compatibility.
        fields[0]["type"] = "long"
        if "default" in fields[0]:
            fields[0]["default"] = 0
    return broken


def compatibility(
    base_url: str,
    subject: str,
    schema: dict[str, object],
    references: list[dict[str, object]] | None = None,
) -> bool:
    """Ask Registry whether a proposed schema can become the next version."""
    encoded_subject = urllib.parse.quote(subject, safe="")
    result = request_json(
        f"{base_url}/compatibility/subjects/{encoded_subject}/versions/latest",
        method="POST",
        payload={
            "schema": json.dumps(schema, separators=(",", ":")),
            "schemaType": "AVRO",
            # Debezium splits reusable nested records into registry subjects.
            # Compatibility validation must resolve the same references as the
            # registered envelope or a valid current schema returns HTTP 422.
            "references": references or [],
        },
    )
    # Confluent API uses is_compatible; tolerate camelCase for API adapters.
    return bool(result.get("is_compatible", result.get("isCompatible", False)))


def main() -> None:
    base_url = required("SCHEMA_REGISTRY_CCOMPAT_URL").rstrip("/")
    topic = f"{required('CDC_TOPIC_PREFIX')}.public.customers"
    # Debezium/Apicurio separates the reusable table-row record from the
    # top-level envelope. The row subject is the contract that changes when a
    # PostgreSQL column changes. Apicurio 3.2.x's ccompat compatibility endpoint
    # cannot independently parse the reference-based envelope (HTTP 422), so we
    # assert that the envelope exists and references the tested row contract,
    # while running positive/negative compatibility checks on the key and row.
    key_subject = f"{topic}-key"
    row_subject = f"{topic}.Value"
    envelope_subject = f"{topic}-value"
    expected_subjects = {key_subject, row_subject, envelope_subject}
    wait_for_subjects(base_url, expected_subjects)

    config = request_json(f"{base_url}/config")
    configured_level = config.get(
        "compatibilityLevel", config.get("compatibility")
    )
    if configured_level != "BACKWARD_TRANSITIVE":
        raise AssertionError(
            "Registry compatibility must be BACKWARD_TRANSITIVE; "
            f"found {configured_level!r}"
        )

    envelope = request_json(
        f"{base_url}/subjects/"
        f"{urllib.parse.quote(envelope_subject, safe='')}/versions/latest"
    )
    referenced_subjects = {
        reference["subject"] for reference in envelope.get("references", [])
    }
    if row_subject not in referenced_subjects:
        raise AssertionError(
            f"Debezium envelope does not reference row contract {row_subject}"
        )

    checked = []
    for subject in (key_subject, row_subject):
        encoded_subject = urllib.parse.quote(subject, safe="")
        latest = request_json(
            f"{base_url}/subjects/{encoded_subject}/versions/latest"
        )
        schema = json.loads(latest["schema"])
        references = latest.get("references", [])
        if not compatibility(base_url, subject, schema, references):
            raise AssertionError(f"Current schema rejected for {subject}")
        if compatibility(
            base_url,
            subject,
            break_first_field_type(schema),
            references,
        ):
            raise AssertionError(
                f"Breaking field-type change was accepted for {subject}"
            )
        checked.append(
            {"subject": subject, "version": latest["version"], "id": latest["id"]}
        )

    print(
        "Schema contract test passed: "
        "compatibility=BACKWARD_TRANSITIVE; "
        f"subjects={json.dumps(checked, separators=(',', ':'))}; "
        f"envelope={envelope_subject}"
    )


if __name__ == "__main__":
    main()
