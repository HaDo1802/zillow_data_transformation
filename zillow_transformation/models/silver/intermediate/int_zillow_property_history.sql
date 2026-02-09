{{ config(
    materialized='incremental',
    unique_key='property_sk',
    on_schema_change='append_new_columns',
    schema='silver'
) }}

select
    *
from {{ ref('stg_zillow_property_master') }}

{% if is_incremental() %}
where ingested_time >
      (select max(ingested_time) from {{ this }})
{% endif %}
