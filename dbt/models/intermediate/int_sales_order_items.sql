{#
  Build the reusable order-item sales grain. Inner join requires a valid order
  header; left join keeps items with no payment attempts and labels them unpaid.
#}
{{ config(materialized='view') }}

select
    orders.order_id,
    order_items.order_item_id,
    orders.customer_id,
    order_items.product_id,
    orders.order_date,
    orders.status as order_status,
    order_items.quantity,
    order_items.unit_price,
    (order_items.quantity * order_items.unit_price)::numeric(14, 2) as sales_amount,
    coalesce (
        payments.payment_status,
        'unpaid'
    ) as payment_status,
    coalesce (
        payments.payment_attempt_count,
        0
    ) as payment_attempt_count,
    -- Incremental marts must react when the order, item or payment aggregate
    -- changes, so propagate the latest platform load time across all inputs.
    greatest(
        orders.loaded_at,
        order_items.loaded_at,
        coalesce(
            payments.source_loaded_at,
            '1900-01-01 00:00:00+00'::timestamptz
        )
    ) as source_loaded_at
from
    {{ ref('stg_orders') }} as orders
    inner join {{ ref('stg_order_items') }} as order_items
        on orders.order_id = order_items.order_id
    left join {{ ref('int_payments_by_order') }} as payments
        on orders.order_id = payments.order_id
