-- Fails if delivery_days is negative or implausibly long (> 365 days).
-- Returns the offending rows (dbt treats any returned row as a test failure).
select
    order_id,
    delivery_days
from {{ ref('mart_orders') }}
where delivery_days is not null
  and (delivery_days < 0 or delivery_days > 365)
