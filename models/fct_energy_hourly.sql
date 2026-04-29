{{ config(materialized='table') }}

WITH prices AS (
    SELECT * FROM {{ ref('stg_prices') }}
),

generation AS (
    -- New layer
    SELECT * FROM {{ ref('stg_generation_hourly') }}
)

SELECT
    -- COALESCE first non-0. 
    -- if there is no price, from time and same backwards.
    COALESCE(p.start_time_local, g.hour) AS start_time_local,
    
    -- Price Data
    p.price_c_kwh,
    CASE WHEN p.price_c_kwh < 0 THEN true ELSE false END AS is_negative_price,
    
    -- Generation Data
    g.wind_mw,
    g.nuclear_mw,
    g.hydro_mw,
    g.other_generation_mw,
    g.total_production_mw

FROM prices p
-- FULL OUTER JOIN
FULL OUTER JOIN generation g
    ON p.start_time_local = g.hour

-- Sort by time
ORDER BY start_time_local DESC
