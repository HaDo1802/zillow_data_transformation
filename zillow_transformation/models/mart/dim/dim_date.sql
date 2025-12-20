select distinct
    to_char(snapshot_date, 'YYYYMMDD')::int as date_id,
    snapshot_date as date_day,
    extract(year from snapshot_date) as year,
    extract(month from snapshot_date) as month,
    extract(day from snapshot_date) as day
from {{ ref('stg_properties') }}
