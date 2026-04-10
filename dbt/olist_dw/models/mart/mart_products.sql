with products as (
    select * from {{ ref('stg_products') }}
),

order_items as (
    select
        product_id,
        count(distinct order_id)        as total_orders,
        count(*)                        as total_items_sold,
        sum(price)                      as total_revenue,
        avg(price)                      as avg_price,
        sum(freight_value)              as total_freight,
        count(distinct seller_id)       as distinct_sellers
    from {{ ref('stg_order_items') }}
    group by product_id
),

final as (
    select
        p.product_id,
        p.category_name_pt,
        p.weight_g,
        p.length_cm,
        p.height_cm,
        p.width_cm,
        p.photos_qty,

        coalesce(oi.total_orders, 0)        as total_orders,
        coalesce(oi.total_items_sold, 0)    as total_items_sold,
        round(coalesce(oi.total_revenue, 0)::numeric, 2) as total_revenue,
        round(coalesce(oi.avg_price, 0)::numeric, 2)     as avg_price,
        round(coalesce(oi.total_freight, 0)::numeric, 2) as total_freight,
        coalesce(oi.distinct_sellers, 0)    as distinct_sellers
    from products p
    left join order_items oi using (product_id)
)

select * from final
