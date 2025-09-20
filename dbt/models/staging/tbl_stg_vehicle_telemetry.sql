with src as (
  select * from {{ source('raw_logistics', 'telematics') }}
)
select
  try_to_number(telemetry_id) as telemetry_id,
  trim(vehicle_id) as vehicle_id,
  try_to_timestamp_ntz(timestamp) as timestamp,
  try_to_decimal(latitude, 10, 6) as latitude,
  try_to_decimal(longitude, 10, 6) as longitude,
  try_to_decimal(speed_mph, 6, 1) * 1.60934 as speed_kmh,  -- Convert mph to kmh
  try_to_number(fuel_level_pct) as fuel_level_percent,
  try_to_number(engine_rpm) as engine_rpm,
  try_to_decimal(engine_temperature_f, 5, 1) as engine_temp_c,  -- Will convert F to C in dimension
  try_to_decimal(odometer_miles, 12, 0) * 1.60934 as odometer_km,  -- Convert miles to km
  -- Calculate fuel consumption from acceleration and speed (simplified)
  case 
    when try_to_decimal(speed_mph, 6, 1) > 0 then 
      round(try_to_decimal(speed_mph, 6, 1) * 0.1 + abs(try_to_decimal(acceleration_g, 4, 2)) * 2, 2)
    else 0
  end as fuel_consumption_lph,
  -- Calculate harsh events from acceleration and brake force
  case when abs(try_to_decimal(acceleration_g, 4, 2)) > 0.3 then 1 else 0 end as harsh_braking_events,
  case when try_to_decimal(acceleration_g, 4, 2) > 0.3 then 1 else 0 end as harsh_acceleration_events,
  case when try_to_decimal(speed_mph, 6, 1) > 65 then 1 else 0 end as speeding_events,  -- 65 mph = ~105 kmh
  -- Calculate idle time (simplified)
  case when try_to_decimal(speed_mph, 6, 1) < 1 then 5 else 0 end as idle_time_minutes,
  -- Generate diagnostic codes based on engine health
  case 
    when try_to_decimal(engine_temperature_f, 5, 1) > 210 then 'P0128'
    when try_to_decimal(battery_voltage, 4, 1) < 12.5 then 'P0562'
    else null
  end as diagnostic_codes,
  -- Calculate engine health score
  case 
    when try_to_decimal(engine_temperature_f, 5, 1) > 210 then 3
    when try_to_decimal(engine_temperature_f, 5, 1) < 180 then 7
    when try_to_decimal(battery_voltage, 4, 1) < 12.5 then 4
    else 9
  end as engine_health_score,
  -- Generate maintenance alert
  case 
    when try_to_decimal(engine_temperature_f, 5, 1) > 210 then true
    when try_to_decimal(battery_voltage, 4, 1) < 12.5 then true
    else false
  end as maintenance_alert,
  current_timestamp() as _ingested_at
from src

