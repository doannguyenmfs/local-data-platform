{{ config(materialized='view') }}
select
    order_id,
    count(*) as payment_attempt_count,
    case
        when bool_and(payment_status = 'completed')
            then 'paid'
        when bool_or(payment_status = 'completed')
            then 'partially_paid'
        else 'unpaid'
    end as payment_status
from {{ ref('stg_payments') }}
group by order_id