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