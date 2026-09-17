{#
  Payment-attempt view. Airflow already maps source `status` to the clearer
  `payment_status` name, so every dbt layer uses one stable column contract.
#}
{{ config(materialized='view') }}

select
    payment_id,
    order_id,
    amount,
    payment_method,
    payment_status,
    paid_at,
    created_at,
    updated_at,
    loaded_at
from {{ source('staging', 'payments') }}
