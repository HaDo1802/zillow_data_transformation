with source as (

    select *
    from {{ source('real_estate_data', 'properties_data_history') }}

)

select
    zillow_property_id,
    snapshot_date,

    price,
    priceChange as price_change,
    bedrooms,
    bathrooms,
    livingArea as living_area,
    lotAreaValue as lot_area,
    Normalized_lotAreaValue as normalized_lot_area,

    propertyType as property_type,
    listingStatus as listing_status,

    rentZestimate as rent_zestimate,
    zestimate,

    street_address,
    city,
    state,
    zip_code,
    vegas_district,
    latitude,
    longitude,

    daysOnZillow as days_on_zillow,
    has3DModel as has_3d_model,
    hasImage as has_image,
    hasVideo as has_video,
    is_fsba,
    is_open_house,

    extracted_at,
    loaded_at

from source
