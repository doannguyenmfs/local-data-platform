{{ config(materialized='table') }}

select
    product_id,
    name,
    category,
    price,
    created_at,
    updated_at
from {{ ref('stg_products') }}