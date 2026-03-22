{{ config(materialized='table') }}

WITH prices AS (
    --  ref() tells dbt: "to take stg_prices"
    SELECT * FROM {{ ref('stg_prices') }}
),

wind AS (
    SELECT * FROM {{ ref('stg_wind') }}
)

SELECT
    p.start_time_local,
    p.price_c_kwh,
    w.avg_wind_mw,
    CASE WHEN p.price_c_kwh < 0 THEN true ELSE false END AS is_negative_price
FROM prices p
LEFT JOIN wind w
    ON p.start_time_local = w.start_time_local
ORDER BY p.start_time_local DESC