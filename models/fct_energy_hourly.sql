{{ config(materialized='table') }}

WITH prices AS (
    SELECT * FROM {{ ref('stg_prices') }}
),

gen AS (
    SELECT * FROM {{ ref('stg_generation_hourly') }}
)

SELECT
    -- Time
    COALESCE(p.start_time_local, gen.hour) AS start_time_local,
    p.price_c_kwh,
    gen.nuclear_mw,
    gen.wind_mw,
    gen.hydro_mw,
    gen.other_generation_mw,
    gen.total_production_mw,
    CASE WHEN p.price_c_kwh < 0 THEN true ELSE false END AS is_negative_price
FROM prices p
FULL OUTER JOIN gen
    ON p.start_time_local = gen.hour
ORDER BY start_time_local DESC
