with orders as (
    select * from {{ ref('stg_orders') }}
    where order_status = 'delivered'
      and delivery_days is not null
),
customers as (
    select * from {{ ref('stg_customers') }}
),
joined as (
    select
        c.state,
        o.delivery_days,
        o.delivered_on_time
    from orders   o
    join customers c on o.customer_id = c.customer_id
),
by_state as (
    select
        state,
        count(*)                                        as total_deliveries,
        round(avg(delivery_days)::numeric, 1)           as avg_delivery_days,
        round(min(delivery_days)::numeric, 1)           as min_delivery_days,
        round(max(delivery_days)::numeric, 1)           as max_delivery_days,
        round(
            100.0 * sum(case when delivered_on_time then 1 end)
            / count(*), 1
        )                                               as on_time_pct
    from joined
    group by 1
)
select * from by_state
order by avg_delivery_days