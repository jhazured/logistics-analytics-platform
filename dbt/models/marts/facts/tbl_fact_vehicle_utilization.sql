with t as (
  select * from {{ ref('tbl_fact_vehicle_telemetry') }}
), d as (
  select date_key, date from {{ ref('tbl_dim_date') }}
)
select
  {{ dbt_utils.generate_surrogate_key(['vehicle_id','d.date_key']) }} as utilization_id,
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

