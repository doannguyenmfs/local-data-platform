{#
  Thin source-conformed view over CDC Silver current state. `source()` records
  that the CDC materializer, not dbt, owns the physical state. This model keeps
  the familiar stg_* naming contract so downstream refs do not know whether a
  source arrived through batch polling or CDC.
#}
{{ config(materialized='view') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    updated_at,
    loaded_at
from {{ source('cdc_silver', 'customers_current') }}
