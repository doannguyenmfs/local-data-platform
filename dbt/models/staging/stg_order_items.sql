{{ config(materialized='view') }}

select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    created_at,
    updated_at,
    loaded_at
from {{ source('staging', 'order_items') }}