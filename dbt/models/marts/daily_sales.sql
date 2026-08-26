{{ config(materialized='table') }}

select
    order_date::date as sales_date,
    count(distinct order_id) as total_orders,
    sum(quantity) as total_items,
    sum(sales_amount)::numeric(16, 2) as total_sales,
    sum(
        case
            when payment_status = 'paid'
                then sales_amount
            else 0
        end
    )::numeric(16, 2) as paid_sales

from {{ ref('fact_sales') }}

group by order_date::date
