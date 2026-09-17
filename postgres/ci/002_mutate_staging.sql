BEGIN;

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
