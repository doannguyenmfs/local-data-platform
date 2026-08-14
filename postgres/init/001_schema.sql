-- ============================================================
-- 001_schema.sql
-- E-commerce OLTP database schema
-- ============================================================


-- ============================================================
-- 1. EXTENSIONS
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;


-- ============================================================
-- 2. CUSTOMERS
-- ============================================================

CREATE TABLE customers (
    customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    first_name VARCHAR(100) NOT NULL,
    last_name  VARCHAR(100) NOT NULL,

    email VARCHAR(255) NOT NULL UNIQUE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ============================================================
-- 3. PRODUCTS
-- ============================================================

CREATE TABLE products (
    product_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    name VARCHAR(255) NOT NULL,

    category VARCHAR(100) NOT NULL,

    price NUMERIC(12, 2) NOT NULL,

    stock_quantity INTEGER NOT NULL DEFAULT 0,

    is_active BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT products_price_positive
        CHECK (price >= 0),

    CONSTRAINT products_stock_non_negative
        CHECK (stock_quantity >= 0)
);


-- ============================================================
-- 4. ORDERS
-- ============================================================

CREATE TABLE orders (
    order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    customer_id UUID NOT NULL,

    status VARCHAR(30) NOT NULL DEFAULT 'pending',

    total_amount NUMERIC(14, 2) NOT NULL DEFAULT 0,

    ordered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT orders_customer_fk
        FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id),

    CONSTRAINT orders_status_check
        CHECK (
            status IN (
                'pending',
                'confirmed',
                'processing',
                'shipped',
                'delivered',
                'cancelled'
            )
        ),

    CONSTRAINT orders_total_amount_non_negative
        CHECK (total_amount >= 0)
);


-- ============================================================
-- 5. ORDER ITEMS
-- ============================================================

CREATE TABLE order_items (
    order_item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    order_id UUID NOT NULL,

    product_id UUID NOT NULL,

    quantity INTEGER NOT NULL,

    unit_price NUMERIC(12, 2) NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT order_items_order_fk
        FOREIGN KEY (order_id)
        REFERENCES orders(order_id)
        ON DELETE CASCADE,

    CONSTRAINT order_items_product_fk
        FOREIGN KEY (product_id)
        REFERENCES products(product_id),

    CONSTRAINT order_items_quantity_positive
        CHECK (quantity > 0),

    CONSTRAINT order_items_unit_price_non_negative
        CHECK (unit_price >= 0),

    CONSTRAINT order_items_order_product_unique
        UNIQUE (order_id, product_id)
);


-- ============================================================
-- 6. PAYMENTS
-- ============================================================

CREATE TABLE payments (
    payment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    order_id UUID NOT NULL,

    amount NUMERIC(14, 2) NOT NULL,

    payment_method VARCHAR(30) NOT NULL,

    status VARCHAR(30) NOT NULL DEFAULT 'pending',

    transaction_id VARCHAR(255),

    paid_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT payments_order_fk
        FOREIGN KEY (order_id)
        REFERENCES orders(order_id),

    CONSTRAINT payments_amount_positive
        CHECK (amount > 0),

    CONSTRAINT payments_method_check
        CHECK (
            payment_method IN (
                'credit_card',
                'debit_card',
                'bank_transfer',
                'e_wallet',
                'cash'
            )
        ),

    CONSTRAINT payments_status_check
        CHECK (
            status IN (
                'pending',
                'completed',
                'failed',
                'refunded'
            )
        )
);


-- ============================================================
-- 7. INDEXES
-- ============================================================

CREATE INDEX idx_orders_customer_id
    ON orders(customer_id);

CREATE INDEX idx_orders_ordered_at
    ON orders(ordered_at);

CREATE INDEX idx_orders_status
    ON orders(status);

CREATE INDEX idx_order_items_order_id
    ON order_items(order_id);

CREATE INDEX idx_order_items_product_id
    ON order_items(product_id);

CREATE INDEX idx_payments_order_id
    ON payments(order_id);

CREATE INDEX idx_payments_created_at
    ON payments(created_at);


-- ============================================================
-- 8. COMMENTS
-- ============================================================

COMMENT ON TABLE customers IS
    'Customer master data';

COMMENT ON TABLE products IS
    'Product catalog';

COMMENT ON TABLE orders IS
    'Customer orders';

COMMENT ON TABLE order_items IS
    'Individual products within an order';

COMMENT ON TABLE payments IS
    'Payment transactions associated with orders';