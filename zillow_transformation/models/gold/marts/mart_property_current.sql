{{
    config(
        materialized='view',
        schema='gold'
    )
}}

-- Join the property dimension to the snapshot fact so the mart exposes a denormalized current-state record.
with joined_property_snapshot as (

    select
        fps.property_id,
        fps.snapshot_date,
        fps.ingested_at,
        dp.street_address,
        dp.city,
        dp.state,
        dp.zip_code,
        dp.vegas_district,
        dp.latitude,
        dp.longitude,
        dp.property_type,
        fps.price,
        fps.zestimate,
        fps.rentzestimate,
        fps.bedrooms,
        fps.bathrooms,
        fps.living_area,
        fps.normalized_lot_area_value,
        fps.normalized_lot_area_unit,
        fps.days_on_zillow,
        fps.listing_status,
        fps.price_per_sqft
    from {{ ref('fact_property_snapshot') }} as fps
    left join {{ ref('dim_property') }} as dp
        on dp.property_id = fps.property_id

),

-- Rank each property history so the mart returns only the latest available snapshot.
ranked_current_rows as (

    select
        *,
        row_number() over (
            partition by property_id
            order by snapshot_date desc, ingested_at desc
        ) as row_num
    from joined_property_snapshot

)

select
    property_id,
    snapshot_date,
    street_address,
    city,
    state,
    zip_code,
    vegas_district,
    latitude,
    longitude,
    property_type,
    price,
    zestimate,
    rentzestimate,
    bedrooms,
    bathrooms,
    living_area,
    normalized_lot_area_value,
    normalized_lot_area_unit,
    days_on_zillow,
    listing_status,
    price_per_sqft
from ranked_current_rows
where row_num = 1
