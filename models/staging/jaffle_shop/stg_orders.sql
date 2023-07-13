with source as(
    select * from {{ source('jaffle_shop', 'order') }}
),

transformed as(
    select 
        id as order_id
        , user_id as customer_id
        , order_date as order_placed_at
        , status as order_status
        , case 
            when status not in('returned','return_pending')
            then order_date 
        end as valid_order_date
        , _etl_loaded_at as loaded_at
    from source
)

select * from transformed
