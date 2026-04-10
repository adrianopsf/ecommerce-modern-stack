with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

monthly as (
    select
        date_trunc('month', purchased_at)::date  as sale_month,
        order_status,

        count(distinct order_id)                  as total_orders,
        sum(total_payment_value)                  as gross_revenue,
        sum(items_total)                          as items_revenue,
        sum(freight_total)                        as freight_revenue,
        avg(total_payment_value)                  as avg_order_value,
        sum(item_count)                           as total_items,
        avg(delivery_days)                        as avg_delivery_days
    from orders
    where purchased_at is not null
    group by
        date_trunc('month', purchased_at),
        order_status
),

final as (
    select
        sale_month,
        order_status,
        total_orders,
        round(gross_revenue::numeric, 2)    as gross_revenue,
        round(items_revenue::numeric, 2)    as items_revenue,
        round(freight_revenue::numeric, 2)  as freight_revenue,
        round(avg_order_value::numeric, 2)  as avg_order_value,
        total_items,
        round(avg_delivery_days::numeric, 1) as avg_delivery_days
    from monthly
)

select * from final
order by sale_month, order_status
