-- set total amount in order table to actual amount in oerder_items
UPDATE orders
SET
	total_amount = calculated.calculated_amount,
	updated_at = NOW()
FROM (SELECT order_id, SUM(quantity * unit_price) as calculated_amount
from order_items
group by order_id) calculated
WHERE orders.order_id = calculated.order_id;

-- after update total amount in orders -> set total amount to payment
UPDATE payments
SET
	amount = 0,
	updated_at = NOW()
FROM orders
WHERE payments.order_id = orders.order_id;
