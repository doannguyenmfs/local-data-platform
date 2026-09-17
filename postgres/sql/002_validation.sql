-- Manual source control-total checks used after data generation. These do not
-- replace automated dbt/Spark tests at downstream grains.

-- Count exact order/item total mismatches.
SELECT COUNT(*) AS mismatched_orders
FROM orders o
JOIN (
    SELECT
        order_id,
        SUM(quantity * unit_price) AS calculated_total
    FROM order_items
    GROUP BY order_id
) oi
    ON o.order_id = oi.order_id
WHERE o.total_amount <> oi.calculated_total;

-- Count material amount differences with an explicit currency tolerance.
SELECT COUNT(*)
FROM orders o
JOIN (
    SELECT
        order_id,
        SUM(quantity * unit_price) AS calculated_total
    FROM order_items
    GROUP BY order_id
) oi
    ON o.order_id = oi.order_id
WHERE ABS(o.total_amount - oi.calculated_total) > 0.01;

-- Inspect the distribution of one versus retry payment attempts.
SELECT
    payment_count,
    COUNT(*) AS order_count
FROM (
    SELECT
        order_id,
        COUNT(*) AS payment_count
    FROM payments
    GROUP BY order_id
) t
GROUP BY payment_count
ORDER BY payment_count;
