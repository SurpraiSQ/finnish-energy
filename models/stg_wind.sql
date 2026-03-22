{{ config(materialized='table') }}

SELECT

    DATE_TRUNC('hour', start_time::timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Helsinki') AS start_time_local,

    ROUND(AVG(value::numeric), 2) AS avg_wind_mw
FROM {{ source('bronze', 'raw_wind_generation') }}
GROUP BY 1
