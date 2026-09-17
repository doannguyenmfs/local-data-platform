-- Current dimension membership must equal current staging membership. This
-- also exposes the still-deferred source->staging hard-delete mapping gap.
with staging_customers as (
    select customer_id
    from {{ ref('stg_customers') }}
),

current_dimension_customers as (
    select customer_id
    from {{ ref('dim_customer') }}
    where is_current
)

select
    coalesce(
        staging_customers.customer_id,
        current_dimension_customers.customer_id
    ) as customer_id,

    case
        when staging_customers.customer_id is null
            then 'missing_in_staging'

        when current_dimension_customers.customer_id is null
            then 'missing_in_dimension'
    end as mismatch_type

from staging_customers

full outer join current_dimension_customers
    on staging_customers.customer_id
        = current_dimension_customers.customer_id

where staging_customers.customer_id is null
   or current_dimension_customers.customer_id is null
