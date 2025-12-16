with source as (

    select *
    from {{ source('real_estate_data', 'properties_data_history') }}

)

select
    -- identifiers
    zillow_property_id,
    snapshot_date,

    -- pricing
    price,
    priceChange as price_change,

    -- property characteristics
    bedrooms,
    bathrooms,
    livingArea as living_area,
    lotAreaValue as lot_size,
    Normalized_lotAreaValue as normalized_lot_size,
    propertyType as property_type,
    listingStatus as listing_status,

    -- estimates
    rentZestimate as rent_zestimate,
    zestimate,

    -- location
    street_address,
    city,
    state,
    zip_code,
    vegas_district,
    latitude,
    longitude,

    -- listing metadata
    daysOnZillow as days_on_zillow,
    has3DModel as has_3d_model,
    hasImage as has_image,
    hasVideo as has_video,
    is_fsba,
    is_open_house,

    -- ingestion metadata
    extracted_at,
    loaded_at

from source
