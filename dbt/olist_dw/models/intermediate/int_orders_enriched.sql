with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select
        order_id,
        sum(payment_value)     as total_payment_value,
        count(*)               as payment_count,
        max(payment_type)      as primary_payment_type,
        max(payment_installments) as max_installments
    from {{ ref('stg_order_payments') }}
    group by order_id
),

items as (
    select
        order_id,
        count(*)           as item_count,
        sum(price)         as items_total,
        sum(freight_value) as freight_total
    from {{ ref('stg_order_items') }}
    group by order_id
),

enriched as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.purchased_at,
        o.approved_at,
        o.delivered_carrier_at,
        o.delivered_customer_at,
        o.estimated_delivery_at,

        coalesce(p.total_payment_value, 0) as total_payment_value,
        coalesce(p.payment_count, 0)       as payment_count,
        p.primary_payment_type,
        coalesce(p.max_installments, 1)    as max_installments,

        coalesce(i.item_count, 0)    as item_count,
        coalesce(i.items_total, 0)   as items_total,
        coalesce(i.freight_total, 0) as freight_total,

        case
            when o.delivered_customer_at is not null and o.purchased_at is not null
            then extract(epoch from (o.delivered_customer_at - o.purchased_at)) / 86400
        end as delivery_days
    from orders o
    left join payments p using (order_id)
    left join items    i using (order_id)
)

select * from enriched
