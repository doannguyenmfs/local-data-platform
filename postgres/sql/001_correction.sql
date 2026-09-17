-- Normalize generated source data after child rows exist. Orders are generated
-- before order_items, so their random provisional amount must be reconciled to
-- the sum of item quantity * unit_price.
UPDATE orders
SET
	total_amount = calculated.calculated_amount,
	updated_at = NOW()
FROM (SELECT order_id, SUM(quantity * unit_price) as calculated_amount
from order_items
group by order_id) calculated
WHERE orders.order_id = calculated.order_id;

-- Payment attempts use the order amount so downstream payment aggregation can
-- focus on status/attempt semantics rather than synthetic amount mismatches.
UPDATE payments
SET
	amount = orders.total_amount,
	updated_at = NOW()
FROM orders
WHERE payments.order_id = orders.order_id;
