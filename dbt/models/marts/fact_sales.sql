{#
  BEGINNER READING MAP
  1. config() tells dbt how the final SELECT is persisted.
  2. sales_order_items selects only rows newer than this table's high-water.
  3. customer_versions resolves the historical customer key at order time.
  4. existing_facts remembers an old date when an item moves to another day.
  5. dbt MERGEs the final SELECT by order_item_id.

  This SQL does not contain an explicit MERGE statement because dbt's
  incremental materialization generates it from materialized/strategy/key.

  Grain: one row per order item. MERGE updates a business row rather than
  appending a duplicate when Airflow retries the same staging batch.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_item_id',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['order_date']},
            {'columns': ['customer_sk']},
            {'columns': ['product_id']},
            {'columns': ['source_loaded_at']}
        ]
    )
}}

{#
  A project upgrade can encounter a relation created before source_loaded_at
  existed.  In that one migration run, read all rows and let dbt add the new
  columns before MERGE.  Normal runs retain the high-water filter.
#}
{% set target_state = namespace(has_source_loaded_at=false) %}
{% if is_incremental() %}
    {% for column in adapter.get_columns_in_relation(this) %}
        {% if column.name | lower == 'source_loaded_at' %}
            {% set target_state.has_source_loaded_at = true %}
        {% endif %}
    {% endfor %}
{% endif %}

with sales_order_items as (

    {# STEP 1: choose the candidate delta from the reusable intermediate view. #}

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
        payment_attempt_count,
        source_loaded_at

    from {{ ref('int_sales_order_items') }}

    {# Business date is not an ingestion cursor: historical orders may change. #}
    {% if is_incremental() and target_state.has_source_loaded_at %}

    where source_loaded_at > coalesce(
        (select max(source_loaded_at) from {{ this }}),
        '1900-01-01 00:00:00+00'::timestamptz
    )

    {% endif %}

),

customer_versions as (

    {# STEP 2: load every SCD2 range needed for the temporal dimension join. #}

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

        -- Seed data may contain orders older than the first captured snapshot.
        -- Rank one deterministic fallback version rather than dropping facts.
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

)

{% if is_incremental() %}

, existing_facts as (

    -- Preserve the previous date only when a changed item crosses a day. The
    -- downstream daily mart uses it to repair the now-stale old partition.
    select
        order_item_id,
        order_date

    from {{ this }}

)

{% endif %}

, resolved_sales as (

    {# STEP 3: enrich each candidate fact and preserve its exact business grain. #}

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

        {% if is_incremental() %}

        case
            when existing_facts.order_date is distinct from sales.order_date
                then existing_facts.order_date
        end as previous_order_date,

        {% else %}

        null::timestamptz as previous_order_date,

        {% endif %}

        sales.order_status,
        sales.quantity,
        sales.unit_price,
        sales.sales_amount,
        sales.payment_status,
        sales.payment_attempt_count,
        sales.source_loaded_at,
        case
            when matching_customer_version.customer_sk is not null
                then 'as_of'
            when earliest_customer_version.customer_sk is not null
                then 'earliest_available'
            else 'unresolved'
        end as customer_key_resolution

    from sales_order_items as sales

    -- Half-open validity ranges ensure one version owns an exact boundary.
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

    {% if is_incremental() %}

    left join existing_facts
        on sales.order_item_id = existing_facts.order_item_id

    {% endif %}

)

select
    {# STEP 4: this result is the source side of dbt's generated MERGE. #}
    order_item_id,
    order_id,
    customer_sk,
    customer_id,
    product_id,
    order_date,
    previous_order_date,
    order_status,
    quantity,
    unit_price,
    sales_amount,
    payment_status,
    payment_attempt_count,
    source_loaded_at,
    customer_key_resolution

from resolved_sales
