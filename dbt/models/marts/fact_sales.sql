{{ config(materialized='table') }}

with sales_order_items as (

    select
        order_id,
        order_item_id,
        customer_id,
        product_id,
        order_date,
        order_status,
        quantity,
        unit_price,
        sales_amount,
        payment_status,
        payment_attempt_count

    from {{ ref('int_sales_order_items') }}

),

customer_versions as (

    select
        customer_sk,
        customer_id,
        valid_from,
        valid_to

    from {{ ref('dim_customer') }}

),

earliest_customer_versions as (

    select
        customer_sk,
        customer_id,
        valid_from

    from (

        select
            customer_sk,
            customer_id,
            valid_from,
            row_number() over (
                partition by customer_id
                order by valid_from, customer_sk
            ) as version_number

        from customer_versions

    ) as ranked_customer_versions

    where version_number = 1

),

resolved_sales as (

    select
        sales.order_id,
        sales.order_item_id,
        sales.customer_id,
        coalesce(
            matching_customer_version.customer_sk,
            earliest_customer_version.customer_sk
        ) as customer_sk,
        sales.product_id,
        sales.order_date,
        sales.order_status,
        sales.quantity,
        sales.unit_price,
        sales.sales_amount,
        sales.payment_status,
        sales.payment_attempt_count,
        case
            when matching_customer_version.customer_sk is not null
                then 'as_of'
            when earliest_customer_version.customer_sk is not null
                then 'earliest_available'
            else 'unresolved'
        end as customer_key_resolution

    from sales_order_items as sales

    left join customer_versions as matching_customer_version
        on sales.customer_id = matching_customer_version.customer_id
        and sales.order_date >= matching_customer_version.valid_from
        and (
            sales.order_date < matching_customer_version.valid_to
            or matching_customer_version.valid_to is null
        )

    left join earliest_customer_versions as earliest_customer_version
        on sales.customer_id = earliest_customer_version.customer_id
        and matching_customer_version.customer_sk is null
        and sales.order_date < earliest_customer_version.valid_from

)

select
    order_item_id,
    order_id,
    customer_sk,
    customer_id,
    product_id,
    order_date,
    order_status,
    quantity,
    unit_price,
    sales_amount,
    payment_status,
    payment_attempt_count,
    customer_key_resolution

from resolved_sales
