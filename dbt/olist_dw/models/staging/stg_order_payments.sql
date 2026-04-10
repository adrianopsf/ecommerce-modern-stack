select
    order_id,
    payment_sequential::integer     as payment_sequential,
    payment_type,
    payment_installments::integer   as installments,
    payment_value::numeric(10, 2)   as payment_value
from {{ source('olist_raw', 'olist_order_payments_dataset') }}
where order_id is not null
