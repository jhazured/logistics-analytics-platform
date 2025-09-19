{{ config(materialized='incremental', unique_key='telemetry_id') }}

with s as (
  select * from {{ ref('stg_vehicle_telemetry') }}
  {% if is_incremental() %}
    where timestamp > (select coalesce(max(timestamp), '1900-01-01') from {{ this }})
  {% endif %}
)
select
  telemetry_id,
  vehicle_id,
  timestamp,
  latitude,
  longitude,
  speed_kmh,
  fuel_level_percent,
  engine_rpm,
  engine_temp_c,
  odometer_km,
  fuel_consumption_lph,
  harsh_braking_events,
  harsh_acceleration_events,
  speeding_events,
  idle_time_minutes,
  diagnostic_codes,
  engine_health_score,
  maintenance_alert
from s

