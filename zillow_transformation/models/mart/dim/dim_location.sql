select distinct
    {{ dbt_utils.generate_surrogate_key([
        'street_address',
        'city',
        'state',
        'zip_code'
    ]) }} as location_id,

    street_address,
    city,
    state,
    zip_code,
    vegas_district,
    latitude,
    longitude

from {{ ref('stg_properties') }}
