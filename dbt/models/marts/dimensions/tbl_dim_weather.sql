{{ config(
    materialized='table',
    tags=['marts', 'dimensions', 'weather', 'load_first']
) }}

with weather_data as (
    select * from {{ ref('tbl_stg_weather_conditions') }}
),

locations as (
    select * from {{ ref('tbl_dim_location') }}
),

weather_enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['weather_id']) }} as weather_id,
        w.location_id,
        w.date as weather_date,
        w.temperature_c,
        w.temperature_f,
        w.precipitation_mm,
        w.precipitation_mm * 0.0393701 as precipitation_inches,
        w.visibility_km,
        w.visibility_km * 0.621371 as visibility_miles,
        w.wind_speed_kmh,
        w.wind_speed_kmh * 0.621371 as wind_speed_mph,
        w.condition as weather_condition,
        -- Calculate weather impact score for logistics operations
        case 
            when w.condition in ('CLEAR', 'PARTLY_CLOUDY') then 0
            when w.condition in ('CLOUDY', 'OVERCAST') then 10
            when w.condition in ('LIGHT_RAIN', 'DRIZZLE') then 25
            when w.condition in ('RAIN', 'HEAVY_RAIN') then 50
            when w.condition in ('SNOW', 'HEAVY_SNOW') then 75
            when w.condition in ('FOG', 'MIST') then 40
            when w.condition in ('STORM', 'THUNDERSTORM') then 90
            else 20
        end as impact_score,
        -- Additional weather severity factors
        case 
            when w.wind_speed_kmh > 50 then 20
            when w.visibility_km < 1 then 30
            when w.precipitation_mm > 10 then 15
            else 0
        end as severity_adjustment,
        -- Final adjusted impact score
        case 
            when w.condition in ('CLEAR', 'PARTLY_CLOUDY') then 0
            when w.condition in ('CLOUDY', 'OVERCAST') then 10
            when w.condition in ('LIGHT_RAIN', 'DRIZZLE') then 25
            when w.condition in ('RAIN', 'HEAVY_RAIN') then 50
            when w.condition in ('SNOW', 'HEAVY_SNOW') then 75
            when w.condition in ('FOG', 'MIST') then 40
            when w.condition in ('STORM', 'THUNDERSTORM') then 90
            else 20
        end + 
        case 
            when w.wind_speed_kmh > 50 then 20
            when w.visibility_km < 1 then 30
            when w.precipitation_mm > 10 then 15
            else 0
        end as adjusted_impact_score,
        w._ingested_at as created_at,
        w._ingested_at as updated_at
    from weather_data w
)

select * from weather_enhanced
