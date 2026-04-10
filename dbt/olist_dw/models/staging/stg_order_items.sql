with source as (
    select * from {{ source('raw', 'order_items') }}
),

renamed as (
    select
        order_id,
        cast(order_item_id as integer)  as order_item_id,
        product_id,
        seller_id,
        cast(shipping_limit_date as timestamp) as shipping_limit_at,
        {{ cents_to_reais('cast(price as numeric)') }} as price,
        {{ cents_to_reais('cast(freight_value as numeric)') }} as freight_value
    from source
    where order_id is not null
)

select * from renamed
