with customer_orders as (
    select * from {{ ref('int_customers_orders') }}
),

final as (
    select
        customer_unique_id,
        city,
        state,
        zip_code_prefix,

        total_orders,
        delivered_orders,
        canceled_orders,

        round(lifetime_value::numeric, 2)   as lifetime_value,
        round(avg_order_value::numeric, 2)  as avg_order_value,
        round(avg_delivery_days::numeric, 1) as avg_delivery_days,

        first_order_at,
        last_order_at,

        case
            when total_orders >= 5 then 'high'
            when total_orders >= 2 then 'medium'
            else 'low'
        end as order_frequency_segment,

        case
            when lifetime_value >= 1000 then 'high_value'
            when lifetime_value >= 300  then 'mid_value'
            else 'low_value'
        end as customer_value_segment
    from customer_orders
)

select * from final
