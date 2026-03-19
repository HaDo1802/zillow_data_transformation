{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- Read the latest silver state so the dimension stays Type 1 and one row per property.
with latest_property_state as (

    select
        zpid as property_id,
        street_address,
        city,
        state,
        zip_code,
        vegas_district,
        latitude,
        longitude,
        propertytype as property_type
    from {{ ref('int_zillow_property_latest') }}

)

select *
from latest_property_state
