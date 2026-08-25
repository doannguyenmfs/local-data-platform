{{ config(materialized='view') }}

select
    product_id,
    name,
    category,
    price,
    created_at,
    updated_at,
    loaded_at
from {{ source('staging', 'products') }}