select distinct on (customer_unique_id)
    customer_unique_id,
    customer_city,
    customer_state,
    total_orders,
    total_spent,
    avg_order_value,
    customer_segment,
    first_order_at,
    last_order_at,
    delivered_orders
from {{ ref('int_customers_orders') }}
order by customer_unique_id, total_spent desc
