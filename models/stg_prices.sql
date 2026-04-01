{{
  config(
    materialized='incremental',
    unique_key='start_time_local'
  )
}}

WITH raw_data AS (
    SELECT
        price AS price_c_kwh,
        start_date AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Helsinki' AS start_time_local
    FROM {{ source('bronze', 'raw_spot_prices') }}
),

deduplicated_data AS (
    SELECT
        price_c_kwh,
        start_time_local,
        -- row numbers
        ROW_NUMBER() OVER(PARTITION BY start_time_local ORDER BY start_time_local) as rn
    FROM raw_data
)

-- Unique rows
SELECT
    price_c_kwh,
    start_time_local
FROM deduplicated_data
WHERE rn = 1

{% if is_incremental() %}
  -- Filter
  AND start_time_local > (SELECT MAX(start_time_local) FROM {{ this }})
{% endif %}
