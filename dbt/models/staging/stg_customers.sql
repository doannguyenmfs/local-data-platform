{{ config(materialized='view') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    updated_at,
    loaded_at
from {{ source('staging', 'customers') }}