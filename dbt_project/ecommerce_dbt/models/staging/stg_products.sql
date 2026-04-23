-- models/staging/stg_products.sql
-- Joins products with English category translations
with products as (
    select * from {{ source('raw', 'raw_products') }}
),
translations as (
    select * from {{ source('raw', 'raw_category_translation') }}
),
cleaned as (
    select
        p.product_id,
        p.product_category_name              as category_pt,
        coalesce(t.product_category_name_english,
                 p.product_category_name)    as category_en,
        p.product_name_lenght::integer        as name_length,
        p.product_description_lenght::integer as description_length,
        p.product_photos_qty::integer         as photo_count,
        p.product_weight_g::numeric           as weight_g,
        p.product_length_cm::numeric          as length_cm,
        p.product_height_cm::numeric          as height_cm,
        p.product_width_cm::numeric           as width_cm
    from products p
    left join translations t
        on p.product_category_name = t.product_category_name
)
select * from cleaned
