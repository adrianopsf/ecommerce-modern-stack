select
    p.product_id,
    coalesce(t.product_category_name_english, 'unknown')  as category_english,
    coalesce(p.product_category_name, 'unknown')           as category_portuguese,
    p.product_photos_qty::integer   as photos_qty,
    p.product_weight_g::numeric     as weight_g,
    p.product_length_cm::numeric    as length_cm,
    p.product_height_cm::numeric    as height_cm,
    p.product_width_cm::numeric     as width_cm
from {{ source('olist_raw', 'olist_products_dataset') }} p
left join {{ source('olist_raw', 'product_category_name_translation') }} t
    on p.product_category_name = t.product_category_name
where p.product_id is not null
