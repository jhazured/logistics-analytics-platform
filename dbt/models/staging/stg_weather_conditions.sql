with src as (
  select * from {{ source('raw', 'weather_data') }}
)
select
  try_to_number(weather_id) as weather_id,
  try_to_date(date) as date,
  try_to_number(hour) as hour,
  try_to_number(location_id) as location_id,
  trim(weather_condition) as condition,
  try_to_decimal(temperature_c, 6, 1) as temperature_c,
  try_to_decimal(temperature_f, 6, 1) as temperature_f,
  try_to_number(humidity_pct) as humidity_percent,
  try_to_decimal(wind_speed_mph, 6, 1) * 1.60934 as wind_speed_kmh,  -- Convert mph to kmh
  try_to_decimal(wind_direction_degrees, 6, 0) as wind_direction_degrees,
  try_to_decimal(precipitation_mm, 6, 1) as precipitation_mm,
  try_to_decimal(visibility_miles, 6, 1) * 1.60934 as visibility_km,  -- Convert miles to km
  trim(weather_description) as weather_description,
  try_to_decimal(pressure_inhg, 6, 2) as pressure_inhg,
  try_to_number(uv_index) as uv_index,
  trim(sunrise_time) as sunrise_time,
  trim(sunset_time) as sunset_time,
  current_timestamp() as _ingested_at
from src

