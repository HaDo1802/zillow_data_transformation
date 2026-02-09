{{
    config(
        materialized='view',
        schema='gold'
    )
}}

with ranked as (

    select
        *,
        row_number() over (
            partition by property_id
            order by snapshot_date desc, digested_time desc nulls last
        ) as row_num
    from {{ ref('fact_property_snapshot') }}

)

select
    property_id,
    snapshot_date,
    digested_time,

    price,
    zestimate,
    rent_zestimate,
    bedrooms,
    bathrooms,
    living_area,
    normalized_lot_area_value,
    normalized_lot_area_unit,
    days_on_zillow,
    listing_status
from ranked
where row_num = 1
