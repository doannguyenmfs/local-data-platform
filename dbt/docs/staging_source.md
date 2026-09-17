{% docs staging_source %}

# Ingestion-owned source boundary

Most tables in the `staging` PostgreSQL schema are loaded incrementally by
Airflow from the ecommerce source schema. Customer data is the deliberate
exception: Debezium captures the PostgreSQL change log and the CDC materializer
owns `cdc_silver.customers_current`.

dbt reads these tables but does not own or load them.

## Ownership boundary

- Airflow owns source-to-staging ingestion for products, orders, order items
  and payments.
- Watermarks control those four batch extracts. The retired
  `staging_customers` watermark is inactive and cannot hold back the DAG.
- CDC Bronze preserves customer change evidence; CDC Silver owns the current
  customer state, including source deletes.
- dbt owns transformations after the staging boundary.
- dbt staging models provide the stable SQL interface used downstream.

## Current ingestion behavior

The pipeline supports:

- Incremental inserts
- Incremental updates
- Idempotent reruns
- Parameterized backfills

Customer inserts, updates and deletes are applied continuously through CDC.
The remaining four Airflow-managed entities use update timestamps and upserts;
their source deletes are not yet propagated. Backfill covers records inside a
chosen time window, while records whose timestamp is incorrectly older than all
processed windows remain an explicit source-contract/late-data limitation.

{% enddocs %}
