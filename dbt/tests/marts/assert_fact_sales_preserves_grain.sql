-- Compare both total and distinct grain counts. A unique test alone would not
-- detect missing order items; this reconciliation protects completeness too.
with intermediate_metrics as (

    select
        count(*) as row_count,
        count(distinct order_item_id) as distinct_grain_count

    from {{ ref('int_sales_order_items') }}

),

fact_metrics as (

    select
        count(*) as row_count,
        count(distinct order_item_id) as distinct_grain_count

    from {{ ref('fact_sales') }}

)

select
    intermediate_metrics.row_count as intermediate_row_count,
    fact_metrics.row_count as fact_row_count,
    intermediate_metrics.distinct_grain_count
        as intermediate_distinct_grain_count,
    fact_metrics.distinct_grain_count
        as fact_distinct_grain_count

from intermediate_metrics

cross join fact_metrics

where intermediate_metrics.row_count
        <> fact_metrics.row_count
   or intermediate_metrics.distinct_grain_count
        <> fact_metrics.distinct_grain_count
