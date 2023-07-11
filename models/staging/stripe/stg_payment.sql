with payments as(
    select
        cast(id	as integer) as payment_id					
        , cast(orderid	as integer) as order_id					
        , cast(paymentmethod as string) as method						
        , cast(status as string) as status						
        , amount/100 as amount					
        , cast(created as date)	as created_at					
        , cast(_batched_at as datetime) as batched_date		
    from {{ source('stripe', 'payment') }}
)
select * from payments