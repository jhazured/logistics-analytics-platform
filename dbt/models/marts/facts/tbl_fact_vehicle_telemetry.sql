{{ config(
    materialized='incremental', 
    unique_key='telemetry_id',
    tags=['marts', 'facts', 'vehicle_telemetry', 'load_second']
) }}

with s as (
  select * from {{ ref('tbl_stg_vehicle_telemetry') }}
  {% if is_incremental() %}
    where timestamp > (select coalesce(max(timestamp), '1900-01-01') from {{ this }})
  {% endif %}
),

-- Join with vehicle dimension for additional context
telemetry_with_dims as (
  select 
    s.*,
    dv.vehicle_type,
    dv.fuel_efficiency_mpg,
    dv.make,
    dv.model,
    dv.vehicle_status
  from s
  left join {{ ref('tbl_dim_vehicle') }} dv on s.vehicle_id = dv.vehicle_id
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
  maintenance_alert,
  -- Dimension context
  vehicle_type,
  fuel_efficiency_mpg,
  make,
  model,
  vehicle_status
from telemetry_with_dims

