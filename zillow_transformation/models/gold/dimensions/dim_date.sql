{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with bounds as (

    select
        min(snapshot_date) as min_date,
        max(snapshot_date) as max_date
    from {{ ref('int_zillow_property_history') }}

),

spine as (

    select
        generate_series(min_date, max_date, interval '1 day')::date as date_day
    from bounds

)

select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day
from spine
