with orders as (
    select * from {{ ref('stg_orders') }}
),
items as (
    select
        order_id,
        count(*)                  as item_count,
        sum(price)                as items_revenue,
        sum(freight_value)        as total_freight,
        sum(total_item_value)     as gross_order_value
    from {{ ref('stg_order_items') }}
    group by 1
),
payments as (
    select
        order_id,
        sum(payment_value)        as total_paid,
        mode() within group (order by payment_type) as payment_type
    from {{ ref('stg_order_payments') }}
    group by 1
),
customers as (
    select * from {{ ref('stg_customers') }}
),
final as (
    select
        o.order_id,
        o.customer_id,
        c.state                             as customer_state,
        c.city                              as customer_city,
        o.order_status,
        o.purchased_at,
        date_trunc('month', o.purchased_at) as purchase_month,
        date_trunc('year',  o.purchased_at) as purchase_year,
        o.delivered_at,
        o.delivery_days,
        o.delivered_on_time,
        i.item_count,
        i.items_revenue,
        i.total_freight,
        i.gross_order_value,
        p.total_paid,
        p.payment_type
    from orders o
    left join items     i on o.order_id    = i.order_id
    left join payments  p on o.order_id    = p.order_id
    left join customers c on o.customer_id = c.customer_id
    where i.gross_order_value is not null    -- ADD THIS LINE
)
select * from final
order by purchased_at