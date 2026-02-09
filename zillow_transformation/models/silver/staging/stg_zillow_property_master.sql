{%- set src = source('raw', 'property_master_data') -%}
{%- set cols = adapter.get_columns_in_relation(src) -%}
{%- set colnames = cols | map(attribute='name') | list -%}

with source as (

    select *
    from {{ src }}

),

mapping as (

    select
        lower(keyword) as keyword,
        district,
        priority
    from {{ ref('district_map') }}

),

base as (

    select
        -- identifiers
        nullif(trim(zpid::text), '') as zillow_property_id,

       case
            when snapshot_date::text ~ '^\d{8}$' then to_date(snapshot_date::text, 'YYYYMMDD')
            else snapshot_date::date
       end as snapshot_date,

        -- timestamps
       trim(extracted_at::text)::timestamptz as extracted_at, 
       trim(ingested_time::text)::timestamptz as ingested_time,
       trim(source_file::text)::text as source_file_key,
     

        -- numeric fields
        (regexp_replace(price::text, ',', '', 'g'))::numeric as price,
        (regexp_replace("priceChange"::text, ',', '', 'g'))::numeric as price_change,
        (regexp_replace(bedrooms::text, ',', '', 'g'))::int as bedrooms,
        (regexp_replace(bathrooms::text, ',', '', 'g'))::numeric as bathrooms,
        (regexp_replace("livingArea"::text, ',', '', 'g'))::numeric as living_area,
        (regexp_replace("lotAreaValue"::text, ',', '', 'g'))::numeric as raw_lot_area_value,
         
        (regexp_replace("rentZestimate"::text, ',', '', 'g'))::numeric as rent_zestimate,
        (regexp_replace(zestimate::text, ',', '', 'g'))::numeric as zestimate,
        (regexp_replace("daysOnZillow"::text, ',', '', 'g'))::int as days_on_zillow,

        -- categorical fields
        (trim("propertyType"::text)) as property_type,
        (trim("listingStatus"::text)) as listing_status,
        nullif(trim("lotAreaUnit"::text), '') as raw_lot_area_unit,
        -- location fields (raw inputs)
        {%- if 'address' in colnames %}
        (trim(address::text)) as address_raw,
        {%- else %}
        null::text as address_raw,
        {%- endif %}

        {%- if 'street_address' in colnames %}
        (trim(street_address::text)) as street_address_raw,
        {%- else %}
        null::text as street_address_raw,
        {%- endif %}

        {%- if 'city' in colnames %}
        nullif(trim(city::text), '') as city_raw,
        {%- else %}
        null::text as city_raw,
        {%- endif %}

        {%- if 'state' in colnames %}
        nullif(trim(state::text), '') as state_raw,
        {%- else %}
        null::text as state_raw,
        {%- endif %}

        {%- if 'zip_code' in colnames %}
        nullif(trim(zip_code::text), '') as zip_code_raw,
        {%- else %}
        null::text as zip_code_raw,
        {%- endif %}

        {%- if 'vegas_district' in colnames %}
        nullif(trim(vegas_district::text), '') as vegas_district_raw,
        {%- else %}
        null::text as vegas_district_raw,
        {%- endif %}

        nullif(regexp_replace(latitude::text, ',', '', 'g'), '')::numeric as latitude,
        nullif(regexp_replace(longitude::text, ',', '', 'g'), '')::numeric as longitude,

        -- raw listing subtype inputs
        nullif(trim("listingSubType"::text), '') as listing_subtype_raw,

        {%- if 'is_fsba' in colnames %}
        nullif(trim(is_fsba::text), '') as is_fsba_raw,
        {%- else %}
        null::text as is_fsba_raw,
        {%- endif %}

        {%- if 'is_open_house' in colnames %}
        nullif(trim(is_open_house::text), '') as is_open_house_raw,
        {%- else %}
        null::text as is_open_house_raw,
        {%- endif %}

        -- raw units / timestamps
        nullif(trim("datePriceChanged"::text), '') as date_price_changed_raw,

        -- boolean fields
        case
            when lower(nullif(trim("has3DModel"::text), '')) in ('true', 't', '1', 'yes', 'y') then true
            when lower(nullif(trim("has3DModel"::text), '')) in ('false', 'f', '0', 'no', 'n') then false
            else null
        end as has_3d_model,

        case
            when lower(nullif(trim("hasImage"::text), '')) in ('true', 't', '1', 'yes', 'y') then true
            when lower(nullif(trim("hasImage"::text), '')) in ('false', 'f', '0', 'no', 'n') then false
            else null
        end as has_image,

        case
            when lower(nullif(trim("hasVideo"::text), '')) in ('true', 't', '1', 'yes', 'y') then true
            when lower(nullif(trim("hasVideo"::text), '')) in ('false', 'f', '0', 'no', 'n') then false
            else null
        end as has_video

    from source

),

address_enriched as (

    select
        base.*,
        coalesce(
            street_address_raw,
            case
                when address_raw is null then null
                when position(', ' in address_raw) > 0 then split_part(address_raw, ', ', 1)
                else address_raw
            end
        ) as street_address,
        coalesce(
            city_raw,
            case
                when address_raw is null then null
                when position(', ' in address_raw) > 0 then split_part(address_raw, ', ', 2)
                else null
            end
        ) as city,
        coalesce(
            state_raw,
            nullif(split_part(split_part(address_raw, ', ', 3), ' ', 1), '')
        ) as state,
        coalesce(
            zip_code_raw,
            nullif(split_part(split_part(address_raw, ', ', 3), ' ', 2), '')
        ) as zip_code
    from base

),

district_enriched as (

    select
        address_enriched.*,
        coalesce(
            vegas_district_raw,
            matched_district.district,
            nullif(trim(address_enriched.city), ''),
            'Las Vegas'
        ) as vegas_district
    from address_enriched
    left join lateral (
        select district
        from mapping m
        where address_enriched.address_raw is not null
          and lower(address_enriched.address_raw) like '%' || m.keyword || '%'
        order by m.priority asc
        limit 1
    ) matched_district on true

),

standardized as (

    select
        district_enriched.*,

        case
            when lower(raw_lot_area_unit) like '%acre%'
             and raw_lot_area_value is not null
                then round(raw_lot_area_value * 43560.0, 2)
            else raw_lot_area_value
        end as normalized_lot_area_value,
        case
            when lower(raw_lot_area_unit) like '%acre%' then 'sqft'
            else coalesce(raw_lot_area_unit, 'sqft')
        end as normalized_lot_area_unit,


        case
            when lower(nullif(trim(is_fsba_raw::text), '')) in ('true', 't', '1', 'yes', 'y') then true
            when lower(nullif(trim(is_fsba_raw::text), '')) in ('false', 'f', '0', 'no', 'n') then false
            when lower(coalesce(listing_subtype_raw, '')) ~ 'is_fsba[^a-z0-9]*(true|t|1|yes|y)' then true
            when lower(coalesce(listing_subtype_raw, '')) ~ 'is_fsba[^a-z0-9]*(false|f|0|no|n)' then false
            else null
        end as is_fsba,

        case
            when lower(nullif(trim(is_open_house_raw::text), '')) in ('true', 't', '1', 'yes', 'y') then true
            when lower(nullif(trim(is_open_house_raw::text), '')) in ('false', 'f', '0', 'no', 'n') then false
            when lower(coalesce(listing_subtype_raw, '')) ~ 'is_openhouse[^a-z0-9]*(true|t|1|yes|y)' then true
            when lower(coalesce(listing_subtype_raw, '')) ~ 'is_openhouse[^a-z0-9]*(false|f|0|no|n)' then false
            else null
        end as is_open_house,

        case
            when date_price_changed_raw is null then null
            when date_price_changed_raw ~ '^\\d+(\\.\\d+)?$'
                then to_timestamp(date_price_changed_raw::double precision / 1000.0)
            else null
        end as date_price_changed_at
    from district_enriched

)

select
    -- Metadata and PRIMARY KEY
    zillow_property_id,
    snapshot_date,
    extracted_at,
    ingested_time,
    source_file_key,

    -- Properties
    price,
    price_change,
    bedrooms,
    bathrooms,
    living_area,
    normalized_lot_area_unit,
    normalized_lot_area_value,
    rent_zestimate,
    zestimate,
    days_on_zillow,

    property_type,
    listing_status,

    street_address,
    city,
    state,
    zip_code,
    vegas_district,
    latitude,
    longitude,

    -- Activity flags
    has_3d_model,
    has_image,
    has_video,
    is_fsba,
    is_open_house,
    date_price_changed_at,

    {{ dbt_utils.generate_surrogate_key([
        'zillow_property_id',
        'snapshot_date'
    ]) }} as property_sk
from standardized
