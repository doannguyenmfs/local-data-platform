-- Full outer join detects missing dates on either side as well as wrong metric
-- values. `IS DISTINCT FROM` is null-safe, unlike ordinary <> comparison.
with expected_daily_sales as (

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

)

select
    coalesce(
        expected_daily_sales.sales_date,
        daily_sales.sales_date
    ) as sales_date,
    expected_daily_sales.total_orders as expected_total_orders,
    daily_sales.total_orders as actual_total_orders,
    expected_daily_sales.total_items as expected_total_items,
    daily_sales.total_items as actual_total_items,
    expected_daily_sales.total_sales as expected_total_sales,
    daily_sales.total_sales as actual_total_sales,
    expected_daily_sales.paid_sales as expected_paid_sales,
    daily_sales.paid_sales as actual_paid_sales

from expected_daily_sales

full outer join {{ ref('daily_sales') }} as daily_sales
    on expected_daily_sales.sales_date = daily_sales.sales_date

where expected_daily_sales.total_orders
        is distinct from daily_sales.total_orders
   or expected_daily_sales.total_items
        is distinct from daily_sales.total_items
   or expected_daily_sales.total_sales
        is distinct from daily_sales.total_sales
   or expected_daily_sales.paid_sales
        is distinct from daily_sales.paid_sales
