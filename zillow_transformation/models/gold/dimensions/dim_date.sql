{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- Build a fixed date spine so the warehouse has a stable calendar regardless of current fact coverage.
with date_spine as (

    select
        generate_series(
            '2024-01-01'::date,
            '2030-12-31'::date,
            '1 day'::interval
        )::date as date_day

)

select
    date_day,
    extract(year from date_day)::integer as year,
    extract(quarter from date_day)::integer as quarter,
    extract(month from date_day)::integer as month,
    trim(to_char(date_day, 'Month')) as month_name,
    extract(week from date_day)::integer as week_of_year,
    extract(day from date_day)::integer as day_of_month,
    extract(isodow from date_day)::integer as day_of_week,
    trim(to_char(date_day, 'Day')) as day_name,
    (extract(isodow from date_day)::integer in (6, 7)) as is_weekend
from date_spine
