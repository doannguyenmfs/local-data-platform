{#
  Thin source-conformed view: keep one customer row per staging business key.
  `source()` records that Airflow/PostgreSQL, not dbt, owns the physical table.
  Business joins/history deliberately happen in later layers.
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
from {{ source('staging', 'customers') }}
