{{
    config(
        materialized='incremental',
        unique_key='property_sk',
        on_schema_change='append_new_columns',
        schema='silver'
    )
}}

-- Read the full silver staging output as the source of truth for historical audit data.
with staged_rows as (

    select *
    from {{ ref('stg_zillow_property_master') }}
    {% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}

)

select *
from staged_rows
