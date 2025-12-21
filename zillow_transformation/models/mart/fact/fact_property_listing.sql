{{
    config(
        materialized='incremental',
        unique_key=['property_id', 'date_id'],
        on_schema_change='append_new_columns'
    )
}}

select
    s.zillow_property_id as property_id,

    {{ dbt_utils.generate_surrogate_key([
        's.street_address',
        's.city',
        's.state',
        's.zip_code'
    ]) }} as location_id,

    d.date_id,

    -- Measures (belong in fact table)
    s.price,
    s.zestimate,
    s.rent_zestimate,
    s.price_change,
    s.days_on_zillow,

    -- Activity flags
    s.has_image,
    s.has_video,
    s.has_3d_model,
    s.is_open_house,
    s.is_fsba,

    -- Audit
    s.snapshot_date,
    s.extracted_at

from {{ ref('stg_properties') }} s

join {{ ref('dim_date') }} d
  on s.snapshot_date = d.date_day

{% if is_incremental() %}
    -- Only load new weekly snapshots
    where s.snapshot_date > (select max(snapshot_date) from {{ this }})
{% endif %}