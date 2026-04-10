-- Asserts that delivery_days is within a plausible range (0–365 days).
-- Returns rows that violate the constraint (failures).
select
    order_id,
    delivery_days
from {{ ref('mart_orders') }}
where delivery_days is not null
  and (delivery_days < 0 or delivery_days > 365)
