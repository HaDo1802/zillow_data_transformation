{{ config(
    materialized='view',
    tags=['dim', 'mart']
) }}

select distinct
    zillow_property_id,
    property_type,
    bedrooms,
    bathrooms,
    living_area,
    lot_size,
    normalized_lot_size
from {{ ref('stg_properties') }}
