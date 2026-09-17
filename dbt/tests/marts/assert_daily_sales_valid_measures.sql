-- Business invariant guard: paid sales is a subset of total sales and no
-- additive daily measure can be negative in this source domain.
select
    sales_date,
    total_orders,
    total_items,
    total_sales,
    paid_sales

from {{ ref('daily_sales') }}

where total_orders < 0
   or total_items < 0
   or total_sales < 0
   or paid_sales < 0
   or paid_sales > total_sales
