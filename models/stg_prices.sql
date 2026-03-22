{{ config(materialized='table') }}

SELECT
    price AS price_c_kwh,
    -- UTC in UTC+2
    start_date AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Helsinki' AS start_time_local
FROM {{ source('bronze', 'raw_spot_prices') }}