{{
    config(
        materialized='incremental',
        unique_key=['property_id', 'snapshot_date'],
        on_schema_change='append_new_columns',
        schema='gold'
    )
}}

-- Read all historical silver events and apply the incremental watermark before deduplication.
with source_history as (

    select
        zpid,
        snapshot_date,
        extracted_at,
        ingested_at,
        price,
        zestimate,
        rentzestimate,
        bedrooms,
        bathrooms,
        livingarea,
        normalized_lot_area_value,
        normalized_lot_area_unit,
        daysonzillow,
        listingstatus
    from {{ ref('int_zillow_property_history') }}
    {% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}

),

-- Deduplicate intraday events so only the latest row per property per snapshot date is kept.
ranked_snapshot_events as (

    select
        *,
        row_number() over (
            partition by zpid, snapshot_date
            order by extracted_at desc, ingested_at desc
        ) as row_num
    from source_history

),

-- Rename fields to gold naming and compute warehouse metrics consumed by downstream marts.
final as (

    select
        zpid as property_id,
        snapshot_date,
        ingested_at,
        price,
        zestimate,
        rentzestimate,
        bedrooms,
        bathrooms,
        livingarea as living_area,
        normalized_lot_area_value,
        normalized_lot_area_unit,
        daysonzillow as days_on_zillow,
        listingstatus as listing_status,
        price::numeric / nullif(livingarea, 0) as price_per_sqft
    from ranked_snapshot_events
    where row_num = 1
      -- Filter records that indicate data quality failures rather than valid edge cases:
      -- missing/non-positive price, missing/non-positive living area, negative lot area,
      -- impossible house-vs-lot size relationships when lot area is present,
      -- missing/negative bedroom or bathroom counts, and missing listing status.
      and price is not null
      and price > 0
      and livingarea is not null
      and livingarea > 0
      and (
            normalized_lot_area_value is null
            or (
                normalized_lot_area_value >= 0
                and livingarea <= normalized_lot_area_value
            )
      )
      and bedrooms is not null
      and bedrooms >= 0
      and bathrooms is not null
      and bathrooms >= 0
      and listingstatus is not null

)

select *
from final
