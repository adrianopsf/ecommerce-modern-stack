with product_sales as (
    select
        oi.product_id,
        count(oi.order_item_id)     as total_sold,
        sum(oi.total_item_value)    as total_revenue,
        avg(oi.price)               as avg_price,
        avg(oi.freight_value)       as avg_freight
    from {{ ref('stg_order_items') }} oi
    inner join {{ ref('mart_orders') }} o on oi.order_id = o.order_id
    group by oi.product_id
)

select
    p.product_id,
    p.category_english,
    p.category_portuguese,
    p.photos_qty,
    p.weight_g,
    coalesce(ps.total_sold, 0)     as total_sold,
    coalesce(ps.total_revenue, 0)  as total_revenue,
    coalesce(ps.avg_price, 0)      as avg_price,
    coalesce(ps.avg_freight, 0)    as avg_freight
from {{ ref('stg_products') }} p
left join product_sales ps on p.product_id = ps.product_id
