{{ config(
    materialized='incremental',
    unique_key='utilization_id',
    on_schema_change='sync_all_columns',
    tags=['marts', 'facts', 'vehicle_utilization', 'load_second']
) }}

with t as (
  select * from {{ ref('tbl_fact_vehicle_telemetry') }}
  {% if is_incremental() %}
    where timestamp > (select coalesce(max(to_date(cast(date_key as string), 'YYYYMMDD')), '1900-01-01') from {{ this }})
  {% endif %}
), d as (
  select date_key, date from {{ ref('tbl_dim_date') }}
)
select
  concat(vehicle_id, '_', cast(d.date_key as string)) as utilization_id,
  vehicle_id,
  d.date_key,
  sum(coalesce(speed_kmh,0)) as total_distance_km, -- proxy; replace with derived distance if available
  round(sum(coalesce(idle_time_minutes,0))/60.0, 1) as total_runtime_hours,
  null as capacity_utilized_percent,
  sum(coalesce(fuel_consumption_lph,0)) as fuel_consumed_liters,
  null as maintenance_hours,
  null as downtime_hours,
  null as utilization_score,
  null as efficiency_score,
  null as cost_per_km,
  null as revenue_per_km
from t
join d on to_date(t.timestamp) = d.date
group by vehicle_id, d.date_key

