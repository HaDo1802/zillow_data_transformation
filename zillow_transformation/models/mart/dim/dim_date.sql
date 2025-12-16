{{ config(materialized='table', tags=['dim', 'mart']) }}

select
    to_char(d::date, 'YYYYMMDD')::int as date_id,
    d::date as date_day,

    extract(dow from d) as day_of_week,
    to_char(d, 'Day') as day_of_week_name,

    date_trunc('week', d)::date as cal_week_start_date,
    extract(day from d) as day_of_month,

    extract(month from d) as cal_month,
    to_char(d, 'Month') as cal_mon_name,
    to_char(d, 'Mon') as cal_mon_name_short,

    extract(quarter from d) as cal_quarter,
    'Q' || extract(quarter from d) as cal_quarter_name,

    extract(year from d) as cal_year,
    (extract(dow from d) in (0,6)) as is_weekend
from generate_series(
    '2015-01-01'::date,
    '2035-12-31'::date,
    interval '1 day'
) as d

