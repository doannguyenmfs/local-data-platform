{% docs staging_source %}

# Airflow-managed staging source

The `staging` PostgreSQL schema is loaded incrementally by Airflow from the
ecommerce source schema.

dbt reads these tables but does not own or load them.

## Ownership boundary

- Airflow owns source-to-staging ingestion.
- Watermarks control incremental extraction.
- dbt owns transformations after the staging boundary.
- dbt staging models provide the stable SQL interface used downstream.

## Current ingestion behavior

The pipeline supports:

- Incremental inserts
- Incremental updates
- Idempotent reruns
- Parameterized backfills

The pipeline does not currently propagate source deletes into staging. It also
does not automatically capture records arriving behind the committed watermark.
Those limitations are documented in the project roadmap.

{% enddocs %}