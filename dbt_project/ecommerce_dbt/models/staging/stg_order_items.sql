-- models/staging/stg_order_items.sql
with source as (
    select * from {{ source('raw', 'raw_order_items') }}
),
cleaned as (
    select
        order_id,
        order_item_id::integer   as line_number,
        product_id,
        seller_id,
        shipping_limit_date::timestamp as shipping_limit_at,
        price::numeric(10,2)     as price,
        freight_value::numeric(10,2) as freight_value,
        (price + freight_value)::numeric(10,2) as total_item_value
    from source
    where order_id is not null
      and price is not null
)
select * from cleaned

