BEGIN;

-- A CDC update creates a second SCD2 version for customer 1. A CDC delete is
-- represented by the Silver tombstone flag, which removes customer 3 from the
-- customers_current view and must close (not erase) its snapshot history.
UPDATE cdc_silver.customers
SET
    last_name = 'Nguyen Updated',
    updated_at = CURRENT_TIMESTAMP,
    source_operation = 'u',
    source_lsn = 4,
    source_ts_ms = (extract(epoch FROM clock_timestamp()) * 1000)::BIGINT,
    kafka_offset = 3,
    applied_at = clock_timestamp()
WHERE customer_id = '00000000-0000-0000-0000-000000000001';

UPDATE cdc_silver.customers
SET
    is_deleted = TRUE,
    source_operation = 'd',
    source_lsn = 5,
    source_ts_ms = (extract(epoch FROM clock_timestamp()) * 1000)::BIGINT,
    kafka_offset = 4,
    applied_at = clock_timestamp()
WHERE customer_id = '00000000-0000-0000-0000-000000000003';

-- A failed attempt becomes completed. This must change order 2 from
-- partially_paid to paid without inserting another fact row.
UPDATE staging.payments
SET
    payment_status = 'completed',
    updated_at = CURRENT_TIMESTAMP + INTERVAL '1 second',
    loaded_at = CURRENT_TIMESTAMP + INTERVAL '1 second'
WHERE payment_id = '40000000-0000-0000-0000-000000000003';

-- Move order 3 from Jan 3 to Jan 4. The incremental daily model must create
-- Jan 4 and remove the now-empty Jan 3 aggregate.
UPDATE staging.orders
SET
    order_date = '2026-01-04 12:00:00+00',
    updated_at = CURRENT_TIMESTAMP + INTERVAL '2 seconds',
    loaded_at = CURRENT_TIMESTAMP + INTERVAL '2 seconds'
WHERE order_id = '20000000-0000-0000-0000-000000000003';

COMMIT;
