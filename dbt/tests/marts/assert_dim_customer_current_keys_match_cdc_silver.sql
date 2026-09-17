{#
  Every live CDC Silver key must have exactly one open SCD2 dimension version,
  and a source-deleted key must have none. FULL OUTER JOIN detects both missing
  and unexpectedly-current keys. dbt singular tests pass only with zero rows.
#}
with silver_keys as (

    select customer_id
    from {{ source('cdc_silver', 'customers_current') }}

),

dimension_keys as (

    select customer_id
    from {{ ref('dim_customer') }}
    where is_current

)

select
    coalesce(silver_keys.customer_id, dimension_keys.customer_id) as customer_id
from silver_keys
full outer join dimension_keys using (customer_id)
where silver_keys.customer_id is null
   or dimension_keys.customer_id is null
