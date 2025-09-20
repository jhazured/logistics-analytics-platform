{{ config(
    materialized='view',
    tags=['staging', 'weather']
) }}

with src as (
  select * from {{ source('raw_logistics', 'WEATHER') }}
)
select
  try_to_number("weather_id") as weather_id,
  try_to_date("date") as date,
  try_to_number("hour") as hour,
  try_to_number("location_id") as location_id,
  trim("weather_condition") as condition,
  "temperature_c" as temperature_c,
  "temperature_f" as temperature_f,
  "humidity_pct" as humidity_percent,
  "wind_speed_mph" * 1.60934 as wind_speed_kmh,  -- Convert mph to kmh
  "wind_direction_degrees" as wind_direction_degrees,
  "precipitation_mm" as precipitation_mm,
  "visibility_miles" * 1.60934 as visibility_km,  -- Convert miles to km
  trim("weather_description") as weather_description,
  "pressure_inhg" as pressure_inhg,
  try_to_number("uv_index") as uv_index,
  trim("sunrise_time") as sunrise_time,
  trim("sunset_time") as sunset_time,
  current_timestamp() as _ingested_at
from src

