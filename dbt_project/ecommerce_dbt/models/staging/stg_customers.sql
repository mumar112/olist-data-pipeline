-- models/staging/stg_customers.sql
with source as (
    select * from {{ source('raw', 'raw_customers') }}
),
cleaned as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix as zip_code,
        customer_city            as city,
        customer_state           as state
    from source
    where customer_id is not null
)
select * from cleaned
