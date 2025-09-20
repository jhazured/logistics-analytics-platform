{{ config(
    materialized='incremental',
    unique_key='performance_id',
    on_schema_change='sync_all_columns',
    tags=['marts', 'facts', 'route_performance', 'load_second']
) }}

with s as (
  select 
    route_id, 
    shipment_id, 
    shipment_date, 
    planned_duration_minutes, 
    actual_duration_minutes, 
    fuel_cost_usd,
    total_cost_usd,
    revenue,
    on_time_delivery_flag,
    is_on_time,
    delivery_status,
    weather_delay_minutes,
    traffic_delay_minutes,
    route_efficiency_score
  from {{ ref('tbl_fact_shipments') }}
  {% if is_incremental() %}
    where shipment_date > (select coalesce(max(to_date(cast(date_key as string), 'YYYYMMDD')), '1900-01-01') from {{ this }})
  {% endif %}
), d as (
  select date_key, date from {{ ref('tbl_dim_date') }}
)
select
  concat(route_id, '_', cast(d.date_key as string)) as performance_id,
  route_id,
  d.date_key,
  null as vehicle_id,
  round(avg(planned_duration_minutes),0) as planned_time_minutes,
  round(avg(actual_duration_minutes),0) as actual_time_minutes,
  round(avg(coalesce(actual_duration_minutes, planned_duration_minutes)) - avg(planned_duration_minutes), 0) as time_variance_minutes,
  round(avg(fuel_cost_usd),2) as planned_fuel_cost,
  round(avg(fuel_cost_usd)*1.05,2) as actual_fuel_cost,
  round((avg(fuel_cost_usd)*1.05) - avg(fuel_cost_usd),2) as fuel_variance,
  sum(case when on_time_delivery_flag = 1 then 1 else 0 end) as on_time_deliveries,
  count(*) as total_deliveries,
  round(100.0 * sum(case when on_time_delivery_flag = 1 then 1 else 0 end) / nullif(count(*),0), 2) as on_time_percentage,
  round(avg(route_efficiency_score),1) as customer_satisfaction_avg,
  round(avg(weather_delay_minutes),0) as weather_delays_minutes,
  round(avg(traffic_delay_minutes),0) as traffic_delays_minutes,
  null as mechanical_delays_minutes,
  round(avg(route_efficiency_score),1) as performance_score
from s
join d on s.shipment_date = d.date
group by route_id, d.date_key

