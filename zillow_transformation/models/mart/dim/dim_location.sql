{{
    config(
        materialized='incremental',
        unique_key='location_id',
        on_schema_change='append_new_columns'
    )
}}

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

{% if is_incremental() %}
    -- Only add new locations
    where {{ dbt_utils.generate_surrogate_key([
        'street_address',
        'city',
        'state',
        'zip_code'
    ]) }} not in (select location_id from {{ this }})
{% endif %}