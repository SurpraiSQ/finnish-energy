{{
  config(
    materialized='incremental',
    unique_key='start_time_local'
  )
}}

SELECT
    price AS price_c_kwh,
    start_date AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Helsinki' AS start_time_local
FROM {{ source('bronze', 'raw_spot_prices') }}

{% if is_incremental() %}
  -- Data filter
  WHERE start_date AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Helsinki' > (select max(start_time_local) from {{ this }})
{% endif %}
