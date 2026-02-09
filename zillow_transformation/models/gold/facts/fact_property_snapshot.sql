{{
    config(
        materialized='incremental',
        unique_key=['property_id', 'snapshot_date'],
        on_schema_change='append_new_columns',
        schema='gold'
    )
}}

with source as (

    select
        zillow_property_id as property_id,
        snapshot_date,
        ingested_time as digested_time,

        price,
        zestimate,
        rent_zestimate,
        bedrooms,
        bathrooms,
        living_area,
        normalized_lot_area_value,
        normalized_lot_area_unit,
        days_on_zillow,
        listing_status
    from {{ ref('int_zillow_property_history') }}

),

{% if is_incremental() %}
max_loaded as (

    select
        coalesce(max(digested_time), '1900-01-01'::date) as max_digested_time
    from {{ this }}

),
{% endif %}

final as (

    select
        *
    from source
    {% if is_incremental() %}
    where digested_time >= (select max_digested_time from max_loaded)
    {% endif %}

)

select
    property_id,
    snapshot_date,
    digested_time,

    price,
    zestimate,
    rent_zestimate,
    bedrooms,
    bathrooms,
    living_area,
    normalized_lot_area_value,
    normalized_lot_area_unit,
    days_on_zillow,
    listing_status
from final
