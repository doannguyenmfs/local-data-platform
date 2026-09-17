{#
  Expose only product attributes approved for analytics. Operational stock and
  active flags remain outside this contract until a downstream requirement
  needs them; selecting every source column would create accidental coupling.
#}
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
