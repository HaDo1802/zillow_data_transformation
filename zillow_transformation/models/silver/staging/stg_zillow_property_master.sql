{{
    config(
        materialized='view',
        schema='silver'
    )
}}

{%- set src = source('raw', 'raw_property_master_data') -%}
{%- set bigint_min = -9223372036854775808 -%}
{%- set bigint_max = 9223372036854775807 -%}

-- Read from the bronze source and only process newly ingested rows on incremental runs.
with source_rows as (

    select *
    from {{ src }}
    {% if is_incremental() %}
    where ingested_at::timestamptz > (select max(ingested_at) from {{ this }})
    {% endif %}

),

-- Standardize raw text fields with trim/null handling before type casting.
trimmed_fields as (

    select
        nullif(trim(zpid), '') as zpid_raw,
        nullif(trim(extracted_at), '') as extracted_at_raw,
        nullif(trim(ingested_at), '') as ingested_at_raw,
        nullif(trim(source_file), '') as source_file_key,

        nullif(trim(address), '') as address,
        nullif(trim(brokername), '') as brokername,
        nullif(trim(lotareaunit), '') as lotareaunit,
        nullif(trim(datepricechanged), '') as datepricechanged,
        nullif(trim(comingsoononmarketdate), '') as comingsoononmarketdate,
        nullif(trim(contingentlistingtype), '') as contingentlistingtype,

        nullif(trim(price), '') as price_raw,
        nullif(trim(pricechange), '') as pricechange_raw,
        nullif(trim(zestimate), '') as zestimate_raw,
        nullif(trim(rentzestimate), '') as rentzestimate_raw,
        nullif(trim(bedrooms), '') as bedrooms_raw,
        nullif(trim(bathrooms), '') as bathrooms_raw,
        nullif(trim(livingarea), '') as livingarea_raw,
        nullif(trim(daysonzillow), '') as daysonzillow_raw,
        nullif(trim(lotareavalue), '') as lotareavalue_raw,
        nullif(trim(latitude), '') as latitude_raw,
        nullif(trim(longitude), '') as longitude_raw,

        upper(nullif(trim(listingstatus), '')) as listingstatus_raw,
        nullif(trim(propertytype), '') as propertytype_raw,
        nullif(trim(listingsubtype), '') as listingsubtype,

        nullif(trim(has3dmodel), '') as has3dmodel_raw,
        nullif(trim(hasimage), '') as hasimage_raw,
        nullif(trim(hasvideo), '') as hasvideo_raw
    from source_rows

),

-- Cast raw text columns into the warehouse-ready silver data types.
typed_fields as (

    select
        case
            when zpid_raw ~ '^\-?\d+$'
             and zpid_raw::numeric between {{ bigint_min }} and {{ bigint_max }}
                then zpid_raw::bigint
            else null
        end as zpid,

        extracted_at_raw::timestamptz as extracted_at,
        ingested_at_raw::timestamptz as ingested_at,
        extracted_at_raw::timestamptz::date as snapshot_date,
        source_file_key,

        case
            when price_raw ~ '^\-?\d+$'
             and price_raw::numeric between {{ bigint_min }} and {{ bigint_max }}
                then price_raw::bigint
            else null
        end as price,

        case
            when pricechange_raw ~ '^\-?\d+$'
             and pricechange_raw::numeric between {{ bigint_min }} and {{ bigint_max }}
                then pricechange_raw::bigint
            else null
        end as pricechange,

        case
            when zestimate_raw ~ '^\-?\d+$'
             and zestimate_raw::numeric between {{ bigint_min }} and {{ bigint_max }}
                then zestimate_raw::bigint
            else null
        end as zestimate,

        case
            when rentzestimate_raw ~ '^\-?\d+$'
             and rentzestimate_raw::numeric between {{ bigint_min }} and {{ bigint_max }}
                then rentzestimate_raw::bigint
            else null
        end as rentzestimate,

        case
            when bedrooms_raw ~ '^\-?\d+$' then bedrooms_raw::integer
            else null
        end as bedrooms,

        case
            when daysonzillow_raw ~ '^\-?\d+$' then daysonzillow_raw::integer
            else null
        end as daysonzillow,

        case
            when bathrooms_raw ~ '^\-?\d+(\.\d+)?$' then bathrooms_raw::numeric(4, 1)
            else null
        end as bathrooms,

        case
            when livingarea_raw ~ '^\-?\d+$' then livingarea_raw::integer
            else null
        end as livingarea,

        case
            when lotareavalue_raw ~ '^\-?\d+(\.\d+)?$' then lotareavalue_raw::numeric
            else null
        end as lotareavalue,

        case
            when latitude_raw ~ '^\-?\d+(\.\d+)?$' then latitude_raw::double precision
            else null
        end as latitude,

        case
            when longitude_raw ~ '^\-?\d+(\.\d+)?$' then longitude_raw::double precision
            else null
        end as longitude,

        case
            when lower(has3dmodel_raw) in ('true', 't', '1', 'yes') then true
            when lower(has3dmodel_raw) in ('false', 'f', '0', 'no') then false
            else null
        end as has3dmodel,

        case
            when lower(hasimage_raw) in ('true', 't', '1', 'yes') then true
            when lower(hasimage_raw) in ('false', 'f', '0', 'no') then false
            else null
        end as hasimage,

        case
            when lower(hasvideo_raw) in ('true', 't', '1', 'yes') then true
            when lower(hasvideo_raw) in ('false', 'f', '0', 'no') then false
            else null
        end as hasvideo,

        listingstatus_raw as listingstatus,
        case
            when upper(replace(propertytype_raw, '-', '_')) in ('SINGLE_FAMILY', 'SINGLEFAMILY') then 'SINGLE_FAMILY'
            when upper(replace(propertytype_raw, '-', '_')) in ('MULTI_FAMILY', 'MULTIFAMILY') then 'MULTI_FAMILY'
            when upper(replace(propertytype_raw, '-', '_')) = 'CONDO' then 'CONDO'
            when upper(replace(propertytype_raw, '-', '_')) = 'TOWNHOUSE' then 'TOWNHOUSE'
            when upper(replace(propertytype_raw, '-', '_')) = 'LOT' then 'LOT'
            when upper(replace(propertytype_raw, '-', '_')) in ('MOBILE', 'MANUFACTURED') then 'MOBILE'
            when upper(replace(propertytype_raw, '-', '_')) = 'COOP' then 'COOP'
            when upper(replace(propertytype_raw, '-', '_')) = 'APARTMENT' then 'APARTMENT'
            when propertytype_raw is null then null
            else 'OTHER'
        end as propertytype,

        address,
        brokername,
        lotareaunit,
        datepricechanged,
        comingsoononmarketdate,
        contingentlistingtype,
        listingsubtype
    from trimmed_fields

),

-- Parse the free-form address string into separate location components.
address_enriched as (

    select
        *,
        split_part(address, ', ', 1) as street_address,
        nullif(split_part(address, ', ', 2), '') as city,
        nullif(split_part(split_part(address, ', ', 3), ' ', 1), '') as state,
        nullif(split_part(split_part(address, ', ', 3), ' ', 2), '') as zip_code
    from typed_fields

),

-- Add silver-level business enrichment for lot size, subtype flags, and timestamp parsing.
business_enriched as (

    select
        *,
        case
            when lower(coalesce(lotareaunit, '')) like '%acre%' and lotareavalue is not null
                then lotareavalue * 43560
            else lotareavalue
        end as normalized_lot_area_value,
        'sqft'::text as normalized_lot_area_unit,
        coalesce(listingsubtype, '') ~ 'is_FSBA.*True' as is_fsba,
        coalesce(listingsubtype, '') ~ 'is_openHouse.*True' as is_open_house,
        case
            when datepricechanged ~ '^\d+(\.\d+)?$'
                then to_timestamp(datepricechanged::double precision / 1000.0)
            else null
        end as date_price_changed_at
    from address_enriched

),

-- Load the district seed so each property can be assigned to the nearest Vegas district.
district_seed as (

    select
        district,
        min_latitude,
        max_latitude,
        min_longitude,
        max_longitude,
        priority
    from {{ ref('district_geo_bbox_map') }}

),

-- Attach the nearest district using a lateral join against seed bbox centers.
district_enriched as (

    select
        business_enriched.*,
        coalesce(nearest_district.district, 'Las Vegas') as vegas_district
    from business_enriched
    left join lateral (
        select district
        from district_seed
        where business_enriched.latitude is not null
          and business_enriched.longitude is not null
        order by
            power(business_enriched.latitude - ((min_latitude + max_latitude) / 2.0), 2)
            + power(business_enriched.longitude - ((min_longitude + max_longitude) / 2.0), 2),
            priority asc
        limit 1
    ) nearest_district on true

),

-- Generate the silver surrogate key and keep only rows with required identifiers and event timestamps.
final as (

    select
        zpid,
        extracted_at,
        ingested_at,
        snapshot_date,
        source_file_key,

        price,
        pricechange,
        zestimate,
        rentzestimate,
        bedrooms,
        bathrooms,
        livingarea,
        daysonzillow,
        lotareavalue,
        latitude,
        longitude,

        has3dmodel,
        hasimage,
        hasvideo,

        listingstatus,
        propertytype,
        address,
        brokername,
        lotareaunit,
        datepricechanged,
        comingsoononmarketdate,
        contingentlistingtype,
        listingsubtype,

        street_address,
        city,
        state,
        zip_code,
        normalized_lot_area_value,
        normalized_lot_area_unit,
        is_fsba,
        is_open_house,
        date_price_changed_at,
        vegas_district,

        {{ dbt_utils.generate_surrogate_key(['zpid', 'source_file_key']) }} as property_sk
    from district_enriched
    where zpid is not null
      and extracted_at is not null

)

select *
from final
