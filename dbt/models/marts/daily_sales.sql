{#
  Grain: one row per sales date. Recalculate only dates touched by new fact
  versions, but MERGE the complete result for each affected date.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='sales_date',
        on_schema_change='sync_all_columns',
        post_hook=[
            "delete from {{ this }} as daily where not exists (select 1 from {{ ref('fact_sales') }} as fact where fact.order_date::date = daily.sales_date)"
        ]
    )
}}

{#
  Upgrade guard: a target created before max_source_loaded_at existed cannot be
  used for incremental filtering. Let dbt sync the column during one full-path
  run, then enable the normal high-water branch on later runs.
#}
{% set target_state = namespace(has_max_source_loaded_at=false) %}
{% if is_incremental() %}
    {% for column in adapter.get_columns_in_relation(this) %}
        {% if column.name | lower == 'max_source_loaded_at' %}
            {% set target_state.has_max_source_loaded_at = true %}
        {% endif %}
    {% endfor %}
{% endif %}

with affected_dates as (

    {% if is_incremental() and target_state.has_max_source_loaded_at %}

    select order_date::date as sales_date
    from {{ ref('fact_sales') }}
    where source_loaded_at > coalesce(
        (select max(max_source_loaded_at) from {{ this }}),
        '1900-01-01 00:00:00+00'::timestamptz
    )

    -- If a fact moves from day A to B, both dates must be recomputed. UNION
    -- removes duplicates when many changed rows touch the same date.
    union

    select previous_order_date::date as sales_date
    from {{ ref('fact_sales') }}
    where source_loaded_at > coalesce(
        (select max(max_source_loaded_at) from {{ this }}),
        '1900-01-01 00:00:00+00'::timestamptz
    )
      and previous_order_date is not null

    {% else %}

    select distinct order_date::date as sales_date
    from {{ ref('fact_sales') }}

    {% endif %}

),

daily_metrics as (

    select
        fact_sales.order_date::date as sales_date,
        count(distinct fact_sales.order_id) as total_orders,
        sum(fact_sales.quantity) as total_items,
        sum(fact_sales.sales_amount)::numeric(16, 2) as total_sales,
        sum(
            case
                when fact_sales.payment_status = 'paid'
                    then fact_sales.sales_amount
                else 0
            end
        )::numeric(16, 2) as paid_sales,
        max(fact_sales.source_loaded_at) as max_source_loaded_at

    from {{ ref('fact_sales') }} as fact_sales

    -- Recompute complete affected days from canonical fact state. Incrementing
    -- only changed amounts would be difficult to reverse safely on retries.
    inner join affected_dates
        on fact_sales.order_date::date = affected_dates.sales_date

    group by fact_sales.order_date::date

)

select
    sales_date,
    total_orders,
    total_items,
    total_sales,
    paid_sales,
    max_source_loaded_at

from daily_metrics
