-- Minimal deterministic fixture for dbt CI. Fixed UUIDs/timestamps make exact
-- post-mutation assertions possible while keeping the workflow fast.
BEGIN;

-- Customer no longer comes from Airflow's polling staging table. This fixture
-- seeds the exact Silver contract dbt reads after CDC cutover.
INSERT INTO cdc_silver.customers (
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    updated_at,
    source_operation,
    source_lsn,
    source_ts_ms,
    kafka_topic,
    kafka_partition,
    kafka_offset
)
VALUES
    (
        '00000000-0000-0000-0000-000000000001',
        'An',
        'Nguyen',
        'an@example.com',
        '2026-01-01 08:00:00+00',
        '2026-01-01 08:00:00+00',
        'r', 1, 1767254400000,
        'ci.public.customers', 0, 0
    ),
    (
        '00000000-0000-0000-0000-000000000002',
        'Binh',
        'Tran',
        'binh@example.com',
        '2026-01-01 09:00:00+00',
        '2026-01-01 09:00:00+00',
        'r', 2, 1767258000000,
        'ci.public.customers', 0, 1
    ),
    (
        -- Customer 3 deliberately has no orders, matching the OLTP foreign-key
        -- rule that permits hard delete only when no order references the key.
        '00000000-0000-0000-0000-000000000003',
        'Chi',
        'Le',
        'chi@example.com',
        '2026-01-01 10:00:00+00',
        '2026-01-01 10:00:00+00',
        'r', 3, 1767261600000,
        'ci.public.customers', 0, 2
    );

-- The baseline manifest on a pull request comes from main and can still point
-- at the pre-cutover polling source. Keeping this tiny compatibility fixture
-- allows CI to build baseline state; current code itself reads CDC Silver.
INSERT INTO staging.customers (
    customer_id, first_name, last_name, email, created_at, updated_at
)
SELECT
    customer_id, first_name, last_name, email, created_at, updated_at
FROM cdc_silver.customers_current;

INSERT INTO staging.products (
    product_id,
    name,
    category,
    price,
    created_at,
    updated_at
)
VALUES
    (
        '10000000-0000-0000-0000-000000000001',
        'CI Laptop',
        'Electronics',
        1000.00,
        '2026-01-01 08:00:00+00',
        '2026-01-01 08:00:00+00'
    ),
    (
        '10000000-0000-0000-0000-000000000002',
        'CI Book',
        'Books',
        20.00,
        '2026-01-01 08:00:00+00',
        '2026-01-01 08:00:00+00'
    );

INSERT INTO staging.orders (
    order_id,
    customer_id,
    order_date,
    status,
    total_amount,
    created_at,
    updated_at
)
VALUES
    (
        '20000000-0000-0000-0000-000000000001',
        '00000000-0000-0000-0000-000000000001',
        '2026-01-02 10:00:00+00',
        'delivered',
        1000.00,
        '2026-01-02 10:00:00+00',
        '2026-01-02 10:00:00+00'
    ),
    (
        '20000000-0000-0000-0000-000000000002',
        '00000000-0000-0000-0000-000000000002',
        '2026-01-02 11:00:00+00',
        'delivered',
        40.00,
        '2026-01-02 11:00:00+00',
        '2026-01-02 11:00:00+00'
    ),
    (
        '20000000-0000-0000-0000-000000000003',
        '00000000-0000-0000-0000-000000000001',
        '2026-01-03 12:00:00+00',
        'confirmed',
        20.00,
        '2026-01-03 12:00:00+00',
        '2026-01-03 12:00:00+00'
    );

INSERT INTO staging.order_items (
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    created_at,
    updated_at
)
VALUES
    (
        '30000000-0000-0000-0000-000000000001',
        '20000000-0000-0000-0000-000000000001',
        '10000000-0000-0000-0000-000000000001',
        1,
        1000.00,
        '2026-01-02 10:00:00+00',
        '2026-01-02 10:00:00+00'
    ),
    (
        '30000000-0000-0000-0000-000000000002',
        '20000000-0000-0000-0000-000000000002',
        '10000000-0000-0000-0000-000000000002',
        2,
        20.00,
        '2026-01-02 11:00:00+00',
        '2026-01-02 11:00:00+00'
    ),
    (
        '30000000-0000-0000-0000-000000000003',
        '20000000-0000-0000-0000-000000000003',
        '10000000-0000-0000-0000-000000000002',
        1,
        20.00,
        '2026-01-03 12:00:00+00',
        '2026-01-03 12:00:00+00'
    );

INSERT INTO staging.payments (
    payment_id,
    order_id,
    amount,
    payment_method,
    payment_status,
    paid_at,
    created_at,
    updated_at
)
VALUES
    -- Order 1 is fully paid; order 2 has completed+failed attempts and begins as
    -- partially_paid; order 3 has no payment and exercises the unpaid default.
    (
        '40000000-0000-0000-0000-000000000001',
        '20000000-0000-0000-0000-000000000001',
        1000.00,
        'credit_card',
        'completed',
        '2026-01-02 10:05:00',
        '2026-01-02 10:00:00+00',
        '2026-01-02 10:05:00+00'
    ),
    (
        '40000000-0000-0000-0000-000000000002',
        '20000000-0000-0000-0000-000000000002',
        20.00,
        'credit_card',
        'completed',
        '2026-01-02 11:05:00',
        '2026-01-02 11:00:00+00',
        '2026-01-02 11:05:00+00'
    ),
    (
        '40000000-0000-0000-0000-000000000003',
        '20000000-0000-0000-0000-000000000002',
        20.00,
        'credit_card',
        'failed',
        NULL,
        '2026-01-02 11:06:00+00',
        '2026-01-02 11:06:00+00'
    );

COMMIT;
