-- Half-open SCD2 intervals must move forward; equal/reversed bounds make an
-- impossible version that cannot be resolved by a temporal join.
select
    customer_sk,
    customer_id,
    valid_from,
    valid_to

from {{ ref('dim_customer') }}

where valid_to is not null
  and valid_to <= valid_from
