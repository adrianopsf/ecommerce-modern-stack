-- Fails if any delivered order has total_payment_value <= 0.
-- Returns the offending rows (dbt treats any returned row as a test failure).
select
    order_id,
    total_payment_value
from {{ ref('mart_orders') }}
where total_payment_value <= 0
