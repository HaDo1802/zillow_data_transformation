{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with ranked as (

    select
        zillow_property_id,
        zillow_property_id as property_id,
        street_address,
        city,
        state,
        zip_code,
        vegas_district,
        latitude,
        longitude,
        property_type,
        ingested_time,
        extracted_at,
        snapshot_date,
        row_number() over (
            partition by zillow_property_id
            order by ingested_time desc nulls last,
                     extracted_at desc nulls last,
                     snapshot_date desc nulls last
        ) as row_num
    from {{ ref('int_zillow_property_history') }}

)

select
    property_id,
    zillow_property_id,
    street_address,
    city,
    state,
    zip_code,
    vegas_district,
    latitude,
    longitude,
    property_type
from ranked
where row_num = 1
