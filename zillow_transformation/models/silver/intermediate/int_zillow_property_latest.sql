{{
    config(
        materialized='view',
        schema='silver'
    )
}}

-- Rank the full property history so the most recent row per property can be selected.
with ranked_history as (

    select
        *,
        row_number() over (
            partition by zpid
            order by snapshot_date desc, extracted_at desc, ingested_at desc
        ) as row_num
    from {{ ref('int_zillow_property_history') }}

)

select
    {{ dbt_utils.star(from=ref('int_zillow_property_history')) }}
from ranked_history
where row_num = 1
