{% snapshot snap_property %}

{{
    config(
        target_schema='real_estate_data',
        unique_key='zillow_property_id',
        strategy='check',
        check_cols=[
            'bedrooms',
            'bathrooms',
            'living_area',
            'lot_area',
            'property_type',
            'listing_status'
        ]
    )
}}

select
    zillow_property_id,
    
    -- Only dimensional attributes (structural changes)
    bedrooms,
    bathrooms,
    living_area,
    lot_area,
    property_type,
    listing_status

from {{ ref('stg_properties') }}

{% endsnapshot %}