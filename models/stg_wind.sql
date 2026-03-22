{{ config(materialized='table') }}

SELECT
    -- Minutes in Hours
    DATE_TRUNC('hour', start_time AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Helsinki') AS start_time_local,
    ROUND(AVG(value), 2) AS avg_wind_mw
FROM {{ source('bronze', 'raw_wind_generation') }}
GROUP BY 1