{{ config(materialized='view', schema='silver') }}

with hourly_avg as (
    -- Step 1: New AVG for each ID
    select
        date_trunc('hour', start_time) as hour,
        dataset_id,
        avg(value) as avg_value
    from {{ source('bronze', 'raw_generation_3min') }}
    group by 1, 2
),

pivoted as (
    -- Step 2: ID in columns
    select
        hour,
        max(case when dataset_id = 188 then avg_value end) as nuclear_mw,
        max(case when dataset_id = 181 then avg_value end) as wind_mw,
        max(case when dataset_id = 191 then avg_value end) as hydro_mw,
        max(case when dataset_id = 192 then avg_value end) as total_production_mw
    from hourly_avg
    group by 1
)

-- Step 3: Final
select
    hour,
    coalesce(nuclear_mw, 0) as nuclear_mw,
    coalesce(wind_mw, 0) as wind_mw,
    coalesce(hydro_mw, 0) as hydro_mw,
    coalesce(total_production_mw, 0) as total_production_mw,
    
    (total_production_mw - (coalesce(nuclear_mw, 0) + coalesce(wind_mw, 0) + coalesce(hydro_mw, 0))) as other_generation_mw
from pivoted
