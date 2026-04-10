-- Asserts that all orders in mart_orders have a non-negative total payment value.
-- Returns rows that violate the constraint (failures).
select
    order_id,
    total_payment_value
from {{ ref('mart_orders') }}
where total_payment_value < 0
