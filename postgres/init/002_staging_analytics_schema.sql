DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS analytics CASCADE;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ==============
-- STAGING DATA
-- ==============
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id UUID PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id UUID PRIMARY KEY,
    name TEXT,
    category TEXT,
    price NUMERIC(12, 2),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.orders (
    order_id UUID PRIMARY KEY,
    customer_id UUID,
    order_date TIMESTAMPTZ,
    status TEXT,
    total_amount NUMERIC(14, 2),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.order_items (
    order_item_id UUID PRIMARY KEY,
    order_id UUID,
    product_id UUID,
    quantity INTEGER,
    unit_price NUMERIC(12, 2),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.payments (
    payment_id UUID PRIMARY KEY,
    order_id UUID,
    amount NUMERIC(14, 2),
    payment_method TEXT,
    payment_status TEXT,
    paid_at TIMESTAMP,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- ==============
-- ANALYTICS DATA
-- ==============
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id UUID NOT NULL,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    record_hash TEXT,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_id UUID PRIMARY KEY,
    name TEXT,
    category TEXT,
    price NUMERIC(12, 2),
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    order_id UUID,
    order_item_id UUID,
    customer_id UUID,
    customer_sk BIGINT,
    product_id UUID,
    order_date TIMESTAMP,
    quantity INTEGER,
    unit_price NUMERIC(12, 2),
    sales_amount NUMERIC(14, 2),
    payment_status TEXT,

    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS analytics.daily_sales (
    sales_date DATE PRIMARY KEY,
    total_orders BIGINT,
    total_items BIGINT,
    total_sales NUMERIC(16, 2),
    paid_sales NUMERIC(16, 2)
);

-- ==============
-- Index for analytic
-- ==============
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_customer_is_current
ON analytics.dim_customer (customer_id)
WHERE is_current = TRUE;
