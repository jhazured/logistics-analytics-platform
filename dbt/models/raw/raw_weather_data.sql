-- Raw weather data from external API
-- This model extracts and standardizes weather data

{{ config(
    materialized='view',
    tags=['raw', 'weather', 'external']
) }}

SELECT 
    weather_id,
    location_id,
    date,
    hour,
    temperature_f,
    temperature_c,
    humidity_pct,
    wind_speed_mph,
    wind_direction_degrees,
    precipitation_mm,
    visibility_miles,
    weather_condition,
    weather_description,
    pressure_inhg,
    uv_index,
    sunrise_time,
    sunset_time,
    created_at,
    _loaded_at
FROM {{ source('raw_logistics', 'weather') }}
WHERE date >= DATEADD('day', -30, CURRENT_DATE())
