with items as (
    select * from {{ ref('stg_order_items') }}
),
products as (
    select * from {{ ref('stg_products') }}
),
orders as (
    select order_id, order_status, purchased_at
    from {{ ref('stg_orders') }}
    where order_status = 'delivered'
),
joined as (
    select
        p.product_id,
        p.category_en,
        i.order_id,
        i.price,
        i.freight_value,
        o.purchased_at
    from items    i
    join products p on i.product_id = p.product_id
    join orders   o on i.order_id   = o.order_id
),
aggregated as (
    select
        category_en,
        count(distinct order_id)              as total_orders,
        count(*)                              as total_items_sold,
        round(sum(price)::numeric, 2)         as total_revenue,
        round(avg(price)::numeric, 2)         as avg_item_price,
        round(sum(freight_value)::numeric, 2) as total_freight,
        round(avg(freight_value)::numeric, 2) as avg_freight
    from joined
    where category_en is not null        -- ADD THIS LINE
    group by 1

)
select * from aggregated
order by total_revenue desc