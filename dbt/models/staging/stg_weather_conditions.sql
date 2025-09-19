with src as (
  select * from {{ source('raw', 'weather_data') }}
)
select
  try_to_number(weather_id) as weather_id,
  try_to_date(date) as date,
  trim(city) as city,
  trim(condition) as condition,
  try_to_decimal(temperature_c, 6, 1) as temperature_c,
  try_to_number(humidity_percent) as humidity_percent,
  try_to_decimal(wind_speed_kmh, 6, 1) as wind_speed_kmh,
  try_to_decimal(precipitation_mm, 6, 1) as precipitation_mm,
  try_to_decimal(visibility_km, 6, 1) as visibility_km
from src

