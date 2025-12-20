{% snapshot snap_property %}

{{
    config(
        target_schema='real_estate_data',
        unique_key='zillow_property_id',
        strategy='check',
        check_cols=[
            'price',
            'bedrooms',
            'bathrooms',
            'living_area',
            'lot_area',
            'property_type',
            'listing_status',
            'rent_zestimate',
            'zestimate'
        ]
    )
}}

select
    zillow_property_id,
    price,
    bedrooms,
    bathrooms,
    living_area,
    lot_area,
    property_type,
    listing_status,
    rent_zestimate,
    zestimate
from {{ ref('stg_properties') }}

{% endsnapshot %}
