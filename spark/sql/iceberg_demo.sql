-- Inspect immutable snapshots and their operations.
SELECT snapshot_id, parent_id, operation, committed_at
FROM local.ecommerce.fact_sales.snapshots
ORDER BY committed_at DESC;

-- Replace <snapshot_id> with an id returned by the query above.
-- SELECT *
-- FROM local.ecommerce.fact_sales VERSION AS OF <snapshot_id>;

-- Iceberg resolves hidden day partitions without exposing a partition column.
SELECT sales_date, SUM(sales_amount) AS total_sales
FROM local.ecommerce.fact_sales
WHERE order_date >= TIMESTAMP '2026-01-01 00:00:00'
  AND order_date < TIMESTAMP '2026-02-01 00:00:00'
GROUP BY sales_date
ORDER BY sales_date;
