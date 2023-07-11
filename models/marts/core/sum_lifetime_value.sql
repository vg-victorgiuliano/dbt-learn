with life_time_sum as(
    select sum(lifetime_value) as soma
    from {{ ref('dim_customer') }}
)
select * from life_time_sum