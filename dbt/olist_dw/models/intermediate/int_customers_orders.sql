with customers as (
    select * from {{ ref('stg_customers') }}
),

orders_enriched as (
    select * from {{ ref('int_orders_enriched') }}
),

customer_aggregates as (
    select
        c.customer_unique_id,
        c.city,
        c.state,
        c.zip_code_prefix,

        count(distinct o.order_id)      as total_orders,
        sum(o.total_payment_value)      as lifetime_value,
        avg(o.total_payment_value)      as avg_order_value,
        min(o.purchased_at)             as first_order_at,
        max(o.purchased_at)             as last_order_at,
        avg(o.delivery_days)            as avg_delivery_days,

        count(distinct case when o.order_status = 'delivered' then o.order_id end)
            as delivered_orders,
        count(distinct case when o.order_status = 'canceled' then o.order_id end)
            as canceled_orders
    from customers c
    left join orders_enriched o using (customer_id)
    group by
        c.customer_unique_id,
        c.city,
        c.state,
        c.zip_code_prefix
)

select * from customer_aggregates
