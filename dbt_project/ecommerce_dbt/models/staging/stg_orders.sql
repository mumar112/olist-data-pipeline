-- models/staging/stg_orders.sql
with source as (
    select * from {{ source('raw', 'raw_orders') }}
),
cleaned as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp::timestamp      as purchased_at,
        order_approved_at::timestamp             as approved_at,
        order_delivered_carrier_date::timestamp  as shipped_at,
        order_delivered_customer_date::timestamp as delivered_at,
        order_estimated_delivery_date::timestamp as estimated_delivery_at,
 
        -- Delivery days (null if not yet delivered)
        case
            when order_delivered_customer_date is not null
            then date_part('day',
                order_delivered_customer_date::timestamp -
                order_purchase_timestamp::timestamp)
        end as delivery_days,
 
        -- Was it delivered on time?
        case
            when order_delivered_customer_date is not null
             and order_estimated_delivery_date is not null
            then order_delivered_customer_date::timestamp
                 <= order_estimated_delivery_date::timestamp
        end as delivered_on_time
 
    from source
    where order_id is not null
)
select * from cleaned


