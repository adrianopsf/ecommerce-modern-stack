with source as (
    select * from {{ source('raw', 'order_payments') }}
),

renamed as (
    select
        order_id,
        cast(payment_sequential as integer)  as payment_sequential,
        payment_type,
        cast(payment_installments as integer) as payment_installments,
        cast(payment_value as numeric)        as payment_value
    from source
    where order_id is not null
)

select * from renamed
