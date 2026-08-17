-- set total amount in order table to actual amount in oerder_items
UPDATE orders
SET
	total_amount = calculated.calculated_amount,
	updated_at = NOW()
FROM (SELECT order_id, SUM(quantity * unit_price) as calculated_amount
from order_items
group by order_id) calculated
WHERE orders.order_id = calculated.order_id;

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

-- after update total amount in orders -> set total amount to payment
UPDATE payments
SET
	amount = orders.total_amount,
	updated_at = NOW()
FROM orders
WHERE payments.order_id = orders.order_id;

SELECT COUNT(*) AS mismatched_payments
FROM payments p
JOIN orders o
    ON p.order_id = o.order_id
WHERE p.amount <> o.total_amount;