-- Raw weather data from external API
-- This model extracts and standardizes weather data
-- Uses incremental loading to minimize Fivetran costs

{{ config(
    materialized='incremental',
    unique_key='weather_id',
    merge_update_columns=['location_id', 'date', 'hour', 'temperature_f', 'temperature_c', 'humidity_pct', 'wind_speed_mph', 'wind_direction_degrees', 'precipitation_mm', 'visibility_miles', 'weather_condition', 'weather_description', 'pressure_inhg', 'uv_index', 'sunrise_time', 'sunset_time', 'created_at', '_loaded_at'],
    tags=['raw', 'weather', 'external', 'incremental']
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

{% if is_incremental() %}
    -- Only process records that are new or updated since last run
    AND _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}
