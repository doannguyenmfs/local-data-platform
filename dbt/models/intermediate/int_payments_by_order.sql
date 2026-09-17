{#
  Collapse one-to-many payment attempts before joining order items. Joining raw
  attempts directly would multiply each sales line and corrupt the fact grain.
  This is a reusable internal building block, so a view avoids copied storage.
#}
{{ config(materialized='view') }}

select
    order_id,
    count(*) as payment_attempt_count,
    max(loaded_at) as source_loaded_at,
    -- All attempts completed => paid; at least one => partially paid; otherwise
    -- unpaid. This definition is project business logic, not a dbt default.
    case
        when bool_and(payment_status = 'completed')
            then 'paid'
        when bool_or(payment_status = 'completed')
            then 'partially_paid'
        else 'unpaid'
    end as payment_status
from {{ ref('stg_payments') }}
group by order_id
