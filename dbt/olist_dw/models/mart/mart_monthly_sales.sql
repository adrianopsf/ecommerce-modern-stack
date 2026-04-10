select
    order_month,
    count(distinct order_id)                                    as total_orders,
    count(distinct customer_id)                                 as unique_customers,
    sum(total_payment_value)                                    as total_revenue,
    avg(total_payment_value)                                    as avg_order_value,
    sum(total_items)                                            as total_items_sold,
    sum(case when is_late_delivery then 1 else 0 end)           as late_deliveries,
    round(
        100.0 * sum(case when is_late_delivery then 1 else 0 end) / count(*),
        2
    )                                                           as late_delivery_pct,
    avg(delivery_days)                                          as avg_delivery_days
from {{ ref('mart_orders') }}
group by order_month
order by order_month
