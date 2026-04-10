with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select
        customer_id,
        count(order_id)                                              as total_orders,
        sum(total_payment_value)                                     as total_spent,
        min(purchased_at)                                            as first_order_at,
        max(purchased_at)                                            as last_order_at,
        avg(total_payment_value)                                     as avg_order_value,
        sum(case when order_status = 'delivered' then 1 else 0 end) as delivered_orders
    from {{ ref('int_orders_enriched') }}
    group by customer_id
)

select
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,

    coalesce(o.total_orders, 0)       as total_orders,
    coalesce(o.total_spent, 0)        as total_spent,
    o.first_order_at,
    o.last_order_at,
    coalesce(o.avg_order_value, 0)    as avg_order_value,
    coalesce(o.delivered_orders, 0)   as delivered_orders,

    case
        when coalesce(o.total_spent, 0) > 500 then 'high_value'
        when coalesce(o.total_spent, 0) > 100 then 'medium_value'
        else                                       'low_value'
    end as customer_segment

from customers c
left join orders o on c.customer_id = o.customer_id
