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
    ) as payment_attempt_count
from
    {{ ref('stg_orders') }} as orders
    inner join {{ ref('stg_order_items') }} as order_items
        on orders.order_id = order_items.order_id
    left join {{ ref('int_payments_by_order') }} as payments
        on orders.order_id = payments.order_id