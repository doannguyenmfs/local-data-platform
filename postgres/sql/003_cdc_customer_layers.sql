-- CDC materialization owns two deliberately different data contracts:
--   Bronze = immutable evidence, one row per physical Kafka record.
--   Silver = latest customer state, one row per business customer key.
-- Keeping both lets us rebuild Silver after a bug without asking PostgreSQL or
-- Debezium to reproduce historical changes that Kafka may already contain.

BEGIN;

CREATE SCHEMA IF NOT EXISTS cdc_bronze;
CREATE SCHEMA IF NOT EXISTS cdc_silver;

CREATE TABLE IF NOT EXISTS cdc_bronze.customer_changes (
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    customer_id UUID NOT NULL,
    -- r=initial snapshot, c=create, u=update, d=delete, t=Kafka tombstone.
    operation CHAR(1) NOT NULL CHECK (operation IN ('r', 'c', 'u', 'd', 't')),
    source_lsn NUMERIC(20, 0),
    source_ts_ms BIGINT,
    transaction_id TEXT,
    event_key JSONB NOT NULL,
    before_value JSONB,
    after_value JSONB,
    kafka_timestamp TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    -- A replay of the same Kafka record can never create a second Bronze row.
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_customer_changes_customer_lsn
    ON cdc_bronze.customer_changes (customer_id, source_lsn, kafka_offset);

CREATE INDEX IF NOT EXISTS idx_customer_changes_ingested_at
    ON cdc_bronze.customer_changes (ingested_at);

CREATE TABLE IF NOT EXISTS cdc_silver.customers (
    customer_id UUID PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    -- Deletes remain visible here as tombstoned current state. Consumers that
    -- need only live rows use customers_current below.
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    source_operation CHAR(1) NOT NULL CHECK (source_operation IN ('r', 'c', 'u', 'd')),
    source_lsn NUMERIC(20, 0) NOT NULL,
    source_ts_ms BIGINT,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_cdc_silver_customers_updated_at
    ON cdc_silver.customers (updated_at);

-- This view is the stable source boundary consumed by dbt. A delete event
-- removes the key from the view without destroying the Silver audit tombstone.
CREATE OR REPLACE VIEW cdc_silver.customers_current AS
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    updated_at,
    applied_at AS loaded_at
FROM cdc_silver.customers
WHERE NOT is_deleted;

-- Existing databases need an online migration; fresh databases receive the
-- same column from postgres/init/003_create_etl_metadata.sql.
ALTER TABLE metadata.etl_watermark
    ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT TRUE;

-- The row is retained for rollback/audit, but Airflow and monitoring no longer
-- treat polling customers as an active pipeline after CDC cutover.
UPDATE metadata.etl_watermark
SET
    active = FALSE,
    candidate_value = NULL,
    updated_at = clock_timestamp()
WHERE pipeline_name = 'staging_customers';

COMMIT;
