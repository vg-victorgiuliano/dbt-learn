with date_table as (
    {{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2016-01-01' as date)",
    end_date="cast(current_timestamp as date)"
   )
}}
),

final as(
    select 
        date_day
        , extract(day from date_day) as day
        , extract(month from date_day) as month
        , extract(year from date_day) as year
        , extract(dayofweek from date_day) as day_of_week
    from date_table
)

select * from final

