select
    s.zillow_property_id,
    d.date_id as snapshot_date_id,
    l.location_id,

    s.price,
    s.bedrooms,
    s.bathrooms,
    s.living_area,
    s.days_on_zillow,
    s.listing_status,
    s.extracted_at
from {{ ref('stg_properties') }} s
join {{ ref('dim_date') }} d
    on s.snapshot_date = d.date_day
join {{ ref('dim_location') }} l
    on s.street_address = l.street_address
   and s.city = l.city
   and s.state = l.state
   and s.zip_code = l.zip_code
