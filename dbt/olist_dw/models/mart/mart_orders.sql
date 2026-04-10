select
    order_id,
    customer_id,
    order_status,
    order_month,
    order_week,
    total_payment_value,
    total_items,
    total_items_value,
    avg_item_price,
    delivery_days,
    is_late_delivery,
    max_installments,
    payment_types_count,
    purchased_at,
    delivered_to_customer_at,
    estimated_delivery_at
from {{ ref('int_orders_enriched') }}
where order_status = 'delivered'
