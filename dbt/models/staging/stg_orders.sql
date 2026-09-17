{#
  Order-header staging contract. The view does not aggregate or join because a
  staging model should preserve source grain and make upstream drift visible.
#}
{{ config(materialized='view') }}

select
    order_id,
    customer_id,
    order_date,
    status,
    total_amount,
    created_at,
    updated_at,
    loaded_at
from {{ source('staging', 'orders') }}
