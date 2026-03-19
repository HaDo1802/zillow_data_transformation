{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- Join the daily property fact to the latest property dimension so market metrics can be sliced by district and type.
with property_market_base as (

    select
        fps.property_id,
        fps.snapshot_date,
        dp.vegas_district,
        dp.property_type,
        fps.listing_status,
        fps.price,
        fps.price_per_sqft,
        fps.bedrooms,
        fps.bathrooms,
        fps.living_area,
        fps.days_on_zillow
    from {{ ref('fact_property_snapshot') }} as fps
    inner join {{ ref('dim_property') }} as dp
        on dp.property_id = fps.property_id

),

-- Keep only rows with the minimum completeness required for stable market pricing and size metrics.
filtered_market_base as (

    select *
    from property_market_base
    where vegas_district is not null
      and price is not null
      and living_area is not null

),

-- Aggregate district-level daily metrics so the app can read pre-computed market summaries directly.
market_summary as (

    select
        vegas_district,
        snapshot_date,

        -- Volume metrics answer how much inventory exists and what stage listings are in on each day.
        count(*) as listing_count,
        count(*) filter (where listing_status = 'FOR_SALE') as for_sale_count,
        count(*) filter (where listing_status = 'SOLD') as sold_count,
        count(*) filter (where listing_status = 'PENDING') as pending_count,

        -- Price metrics answer what the market costs overall and on a per-square-foot basis.
        avg(price) as avg_price,
        percentile_cont(0.5) within group (order by price) as median_price,
        min(price) as min_price,
        max(price) as max_price,
        avg(price_per_sqft) as avg_price_per_sqft,
        percentile_cont(0.5) within group (order by price_per_sqft) as median_price_per_sqft,

        -- Size metrics answer what a typical home looks like in each district on each day.
        avg(bedrooms) as avg_bedrooms,
        avg(bathrooms) as avg_bathrooms,
        avg(living_area) as avg_living_area,

        -- Market velocity metrics answer how quickly listings are moving through the market.
        avg(days_on_zillow) as avg_days_on_zillow,
        percentile_cont(0.5) within group (order by days_on_zillow) as median_days_on_zillow,

        -- Property type breakdown answers what mix of housing inventory makes up each district-day snapshot.
        count(*) filter (where property_type = 'SINGLE_FAMILY') as single_family_count,
        count(*) filter (where property_type = 'CONDO') as condo_count,
        count(*) filter (where property_type = 'TOWNHOUSE') as townhouse_count,
        count(*) filter (where property_type = 'MULTI_FAMILY') as multi_family_count
    from filtered_market_base
    group by
        vegas_district,
        snapshot_date

)

select *
from market_summary
