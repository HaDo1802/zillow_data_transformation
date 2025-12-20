select
    zillow_property_id as property_id,

    price,
    bedrooms,
    bathrooms,
    living_area,
    lot_area,
    property_type,
    listing_status,
    rent_zestimate,
    zestimate

from {{ ref('snap_property') }}
where dbt_valid_to is null
