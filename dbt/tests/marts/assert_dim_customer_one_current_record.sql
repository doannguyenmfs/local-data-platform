-- SCD2 invariant: no business customer can have more than one open version.
-- An active Silver key is required to have one by the companion
-- assert_dim_customer_current_keys_match_cdc_silver test. A source-deleted key
-- correctly has zero open versions while retaining closed history.
select
    customer_id,
    count(*) filter (
        where is_current
    ) as current_record_count
from {{ ref('dim_customer') }}
group by customer_id
having count(*) filter (
    where is_current
) > 1
