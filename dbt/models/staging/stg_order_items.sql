{#
  Preserve the order-item grain used by fact_sales. Generic unique/not-null and
  relationship tests in _staging_models.yml defend this boundary.
#}
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
