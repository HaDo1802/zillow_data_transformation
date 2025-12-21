select
    zillow_property_id as property_id,

    bedrooms,
    bathrooms,
    living_area,
    lot_area,
    property_type,
    listing_status,

    -- SCD metadata
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    (dbt_valid_to is null) as is_current

from {{ ref('snap_property') }}