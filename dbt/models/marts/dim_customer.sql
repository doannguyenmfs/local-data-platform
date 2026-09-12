{{ config(materialized='view') }}

select
    dbt_scd_id as customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current,
    created_at,
    updated_at
from
    {{ ref('snap_customers') }}