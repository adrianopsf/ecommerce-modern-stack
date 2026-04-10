with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select
        order_id,
        sum(payment_value)               as total_payment_value,
        count(distinct payment_type)     as payment_types_count,
        max(installments)                as max_installments
    from {{ ref('stg_order_payments') }}
    group by order_id
),

items as (
    select
        order_id,
        count(order_item_id)             as total_items,
        sum(total_item_value)            as total_items_value,
        avg(price)                       as avg_item_price
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.*,

    coalesce(p.total_payment_value, 0)  as total_payment_value,
    coalesce(p.payment_types_count, 0)  as payment_types_count,
    coalesce(p.max_installments, 1)     as max_installments,

    coalesce(i.total_items, 0)          as total_items,
    coalesce(i.total_items_value, 0)    as total_items_value,
    coalesce(i.avg_item_price, 0)       as avg_item_price,

    case
        when o.delivered_to_customer_at is not null
        then extract(day from o.delivered_to_customer_at - o.purchased_at)::integer
        else null
    end as delivery_days,

    case
        when o.delivered_to_customer_at > o.estimated_delivery_at then true
        else false
    end as is_late_delivery,

    to_char(o.purchased_at, 'YYYY-MM')                as order_month,
    date_trunc('week', o.purchased_at)::date          as order_week

from orders o
left join payments p on o.order_id = p.order_id
left join items    i on o.order_id = i.order_id
