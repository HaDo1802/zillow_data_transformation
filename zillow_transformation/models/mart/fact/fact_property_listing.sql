select
    s.zillow_property_id as property_id,

    {{ dbt_utils.generate_surrogate_key([
        's.street_address',
        's.city',
        's.state',
        's.zip_code'
    ]) }} as location_id,

    d.date_id,

    s.price,
    s.days_on_zillow,
    s.has_image,
    s.has_video,
    s.has_3d_model,
    s.is_open_house

from {{ ref('stg_properties') }} s

join {{ ref('dim_date') }} d
  on s.snapshot_date = d.date_day
