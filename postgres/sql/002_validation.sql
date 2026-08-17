-- check total amount mismatch
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

-- validate orphan
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

-- validate paymet distribution
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