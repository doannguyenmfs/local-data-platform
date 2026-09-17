-- Singular dbt tests pass when this query returns zero rows. The one-to-one
-- count check proves payment enrichment did not multiply or drop order items.
with source_count as (
    select count(*) as row_count
    from {{ ref('stg_order_items') }}
),

intermediate_count as (
    select count(*) as row_count
    from {{ ref('int_sales_order_items') }}
)

select
    source_count.row_count as source_row_count,
    intermediate_count.row_count as intermediate_row_count
from
    source_count
    cross join intermediate_count
where
    source_count.row_count <> intermediate_count.row_count
