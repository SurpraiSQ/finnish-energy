{{ config(materialized='view', schema='silver') }}
    
WITH base AS (
    SELECT
        -- Step 1: New AVG for each ID
        DATE_TRUNC('hour', start_time) AS hour,
        dataset_id,
        value
    FROM {{ source('bronze', 'raw_generation_3min') }}
),

pivoted AS (
    SELECT
        hour,
        -- Step 2: ID in columns
        AVG(CASE WHEN dataset_id = 188 THEN value END) AS nuclear_mw,
        AVG(CASE WHEN dataset_id = 181 THEN value END) AS wind_mw,
        AVG(CASE WHEN dataset_id = 191 THEN value END) AS hydro_mw,
        AVG(CASE WHEN dataset_id = 192 THEN value END) AS total_production_mw
    FROM base
    GROUP BY 1
)

SELECT
    hour,
    ROUND(CAST(nuclear_mw AS NUMERIC), 2) AS nuclear_mw,
    ROUND(CAST(wind_mw AS NUMERIC), 2) AS wind_mw,
    ROUND(CAST(hydro_mw AS NUMERIC), 2) AS hydro_mw,
    ROUND(CAST(total_production_mw AS NUMERIC), 2) AS total_production_mw,
    
    -- Step 3: Final
    ROUND(CAST(
        total_production_mw - (
            COALESCE(nuclear_mw, 0) + 
            COALESCE(wind_mw, 0) + 
            COALESCE(hydro_mw, 0)
        ) AS NUMERIC), 2) AS other_generation_mw
FROM pivoted
