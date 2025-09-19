with src as (
  select * from {{ source('raw', 'telematics_data') }}
)
select
  try_to_number(telemetry_id) as telemetry_id,
  trim(vehicle_id) as vehicle_id,
  try_to_timestamp_ntz(timestamp) as timestamp,
  try_to_decimal(latitude, 10, 6) as latitude,
  try_to_decimal(longitude, 10, 6) as longitude,
  try_to_decimal(speed_kmh, 6, 1) as speed_kmh,
  try_to_number(fuel_level_percent) as fuel_level_percent,
  try_to_number(engine_rpm) as engine_rpm,
  try_to_decimal(engine_temp_c, 5, 1) as engine_temp_c,
  try_to_number(odometer_km) as odometer_km,
  try_to_decimal(fuel_consumption_lph, 6, 2) as fuel_consumption_lph,
  try_to_number(harsh_braking_events) as harsh_braking_events,
  try_to_number(harsh_acceleration_events) as harsh_acceleration_events,
  try_to_number(speeding_events) as speeding_events,
  try_to_number(idle_time_minutes) as idle_time_minutes,
  diagnostic_codes,
  try_to_decimal(engine_health_score, 4, 1) as engine_health_score,
  iff(lower(trim(maintenance_alert)) in ('true','1','yes'), true, false) as maintenance_alert
from src

