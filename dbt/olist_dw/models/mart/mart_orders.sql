with orders_enriched as (
    select * from {{ ref('int_orders_enriched') }}
),

customers as (
    select customer_id, customer_unique_id, city, state
    from {{ ref('stg_customers') }}
),

sellers as (
    select
        order_id,
        min(seller_id) as primary_seller_id
    from {{ ref('stg_order_items') }}
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        c.customer_unique_id,
        c.city          as customer_city,
        c.state         as customer_state,
        s.primary_seller_id,

        o.order_status,
        o.purchased_at,
        o.approved_at,
        o.delivered_carrier_at,
        o.delivered_customer_at,
        o.estimated_delivery_at,

        o.total_payment_value,
        o.payment_count,
        o.primary_payment_type,
        o.max_installments,

        o.item_count,
        o.items_total,
        o.freight_total,
        o.delivery_days,

        case
            when o.delivered_customer_at <= o.estimated_delivery_at then true
            when o.delivered_customer_at > o.estimated_delivery_at  then false
        end as delivered_on_time
    from orders_enriched o
    left join customers c using (customer_id)
    left join sellers   s using (order_id)
)

select * from final
