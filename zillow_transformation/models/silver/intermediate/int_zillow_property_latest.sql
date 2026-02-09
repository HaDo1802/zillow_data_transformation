{{ config(materialized='view', schema='silver') }}

with ranked as (

    select
        *,
        row_number() over (
            partition by zillow_property_id
            order by snapshot_date desc nulls last, extracted_at desc nulls last, ingested_time desc nulls last
        ) as row_num
    from {{ ref('int_zillow_property_history') }}

)

select
    *
from ranked
where row_num = 1
