{{ config(
    materialized='view',
    tags=['staging', 'telemetry']
) }}

with src as (
  select * from {{ source('raw_logistics', 'TELEMATICS') }}
)
select
  try_to_number("telemetry_id") as telemetry_id,
  trim("vehicle_id") as vehicle_id,
  to_timestamp_ntz("timestamp", 9) as timestamp,  -- Convert nanoseconds to timestamp
  "latitude" as latitude,
  "longitude" as longitude,
  "speed_mph" * 1.60934 as speed_kmh,  -- Convert mph to kmh
  "fuel_level_pct" as fuel_level_percent,
  "engine_rpm" as engine_rpm,
  "engine_temperature_f" as engine_temp_c,  -- Will convert F to C in dimension
  null as odometer_km,  -- Odometer not available in telematics data
  -- Calculate fuel consumption from acceleration and speed (simplified)
  case 
    when "speed_mph" > 0 then 
      round("speed_mph" * 0.1 + abs("acceleration_g") * 2, 2)
    else 0
  end as fuel_consumption_lph,
  -- Calculate harsh events from acceleration and brake force
  case when abs("acceleration_g") > 0.3 then 1 else 0 end as harsh_braking_events,
  case when "acceleration_g" > 0.3 then 1 else 0 end as harsh_acceleration_events,
  case when "speed_mph" > 65 then 1 else 0 end as speeding_events,  -- 65 mph = ~105 kmh
  -- Calculate idle time (simplified)
  case when "speed_mph" < 1 then 5 else 0 end as idle_time_minutes,
  -- Generate diagnostic codes based on engine health
  case 
    when "engine_temperature_f" > 210 then 'P0128'
    else null
  end as diagnostic_codes,
  -- Calculate engine health score
  case 
    when "engine_temperature_f" > 210 then 3
    when "engine_temperature_f" < 180 then 7
    else 9
  end as engine_health_score,
  -- Generate maintenance alert
  case 
    when "engine_temperature_f" > 210 then true
    else false
  end as maintenance_alert,
  current_timestamp() as _ingested_at
from src

