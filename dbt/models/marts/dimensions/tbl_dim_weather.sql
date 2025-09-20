{{ config(materialized='table') }}

with weather_data as (
    select * from {{ source('raw_logistics', 'WEATHER') }}
),

locations as (
    select * from {{ ref('tbl_dim_location') }}
),

weather_enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['weather_id']) }} as weather_id,
        w.location_id,
        w.weather_date,
        w.temperature_c,
        (w.temperature_c * 9/5) + 32 as temperature_f,
        w.precipitation_mm,
        w.precipitation_mm * 0.0393701 as precipitation_inches,
        w.visibility_km,
        w.visibility_km * 0.621371 as visibility_miles,
        w.wind_speed_kmh,
        w.wind_speed_kmh * 0.621371 as wind_speed_mph,
        w.weather_condition,
        -- Calculate weather impact score for logistics operations
        case 
            when w.weather_condition in ('CLEAR', 'PARTLY_CLOUDY') then 0
            when w.weather_condition in ('CLOUDY', 'OVERCAST') then 10
            when w.weather_condition in ('LIGHT_RAIN', 'DRIZZLE') then 25
            when w.weather_condition in ('RAIN', 'HEAVY_RAIN') then 50
            when w.weather_condition in ('SNOW', 'HEAVY_SNOW') then 75
            when w.weather_condition in ('FOG', 'MIST') then 40
            when w.weather_condition in ('STORM', 'THUNDERSTORM') then 90
            else 20
        end as impact_score,
        -- Additional weather severity factors
        case 
            when w.wind_speed_kmh > 50 then impact_score + 20
            when w.visibility_km < 1 then impact_score + 30
            when w.precipitation_mm > 10 then impact_score + 15
            else impact_score
        end as adjusted_impact_score,
        w.created_at,
        w.updated_at
    from weather_data w
)

select * from weather_enhanced
