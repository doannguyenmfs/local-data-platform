{#
  Current-state product dimension. It is a table because BI repeatedly reads it
  and the projection is small; unlike customer, no historical product versions
  are required by the current business scope. Transaction price lives in fact.
#}
{{ config(materialized='table') }}

select
    product_id,
    name,
    category,
    price,
    created_at,
    updated_at
from {{ ref('stg_products') }}
