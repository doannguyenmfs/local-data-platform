{#
  Present dbt snapshot metadata as a business-facing SCD2 dimension. A view is
  sufficient because the snapshot is already materialized history and this
  model only renames/derives columns; a table would duplicate all versions.
#}
{{ config(materialized='view') }}

select
    -- dbt creates a version identifier; this model assigns it the semantic role
    -- of a customer-version surrogate key.
    dbt_scd_id as customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    -- Null upper bound is dbt's representation of the open/current interval.
    dbt_valid_to is null as is_current,
    created_at,
    updated_at
from
    {{ ref('snap_customers') }}
