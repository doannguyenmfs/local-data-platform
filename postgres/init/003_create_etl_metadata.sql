-- Pipeline control state lives outside staging so replacing/rebuilding landing
-- relations cannot silently reset progress. candidate_value is a prepared but
-- uncommitted upper bound; watermark_value is safe for all downstream users.
DROP SCHEMA IF EXISTS metadata CASCADE;
CREATE SCHEMA IF NOT EXISTS metadata;
CREATE TABLE IF NOT EXISTS metadata.etl_watermark (
    pipeline_name TEXT PRIMARY KEY,
    watermark_value TIMESTAMPTZ NOT NULL,
    candidate_value TIMESTAMPTZ,
    -- Inactive rows are retained as migration/rollback evidence but are not
    -- part of the Airflow commit barrier or freshness alert set.
    active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO metadata.etl_watermark (
    pipeline_name,
    watermark_value,
    active
)
VALUES
    -- Epoch makes the first scheduled run equivalent to an initial full load
    -- while keeping exactly the same incremental code path.
    (
        'staging_customers',
        '1970-01-01 00:00:00+00'::TIMESTAMPTZ,
        FALSE
    ),
    (
        'staging_products',
        '1970-01-01 00:00:00+00'::TIMESTAMPTZ,
        TRUE
    ),
    (
        'staging_orders',
        '1970-01-01 00:00:00+00'::TIMESTAMPTZ,
        TRUE
    ),
    (
        'staging_order_items',
        '1970-01-01 00:00:00+00'::TIMESTAMPTZ,
        TRUE
    ),
    (
        'staging_payments',
        '1970-01-01 00:00:00+00'::TIMESTAMPTZ,
        TRUE
    )
ON CONFLICT (pipeline_name)
DO NOTHING;
