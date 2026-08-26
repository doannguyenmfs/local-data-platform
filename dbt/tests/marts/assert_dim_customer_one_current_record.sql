select
    customer_id,
    count(*) filter (
        where is_current
    ) as current_record_count
from {{ ref('dim_customer') }}
group by customer_id
having count(*) filter (
    where is_current
) <> 1