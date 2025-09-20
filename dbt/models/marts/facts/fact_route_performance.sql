{{ config(
    materialized='incremental',
    unique_key='performance_id',
    on_schema_change='sync_all_columns'
) }}

with s as (
  select route_id, shipment_id, shipment_date, planned_duration_minutes, actual_duration_minutes, fuel_cost, delivery_status, customer_rating
  from {{ ref('fact_shipments') }}
  {% if is_incremental() %}
    where shipment_date > (select coalesce(max(performance_date), '1900-01-01') from {{ this }})
  {% endif %}
), d as (
  select date_key, date from {{ ref('dim_date') }}
)
select
  {{ dbt_utils.generate_surrogate_key(['route_id','d.date_key']) }} as performance_id,
  route_id,
  d.date_key,
  null as vehicle_id,
  round(avg(planned_duration_minutes),0) as planned_time_minutes,
  round(avg(actual_duration_minutes),0) as actual_time_minutes,
  round(avg(coalesce(actual_duration_minutes, planned_duration_minutes)) - avg(planned_duration_minutes), 0) as time_variance_minutes,
  round(avg(fuel_cost),2) as planned_fuel_cost,
  round(avg(fuel_cost)*1.05,2) as actual_fuel_cost,
  round((avg(fuel_cost)*1.05) - avg(fuel_cost),2) as fuel_variance,
  sum(case when delivery_status = 'Delivered' then 1 else 0 end) as on_time_deliveries,
  count(*) as total_deliveries,
  round(100.0 * sum(case when delivery_status = 'Delivered' then 1 else 0 end) / nullif(count(*),0), 2) as on_time_percentage,
  round(avg(customer_rating),1) as customer_satisfaction_avg,
  null as weather_delays_minutes,
  null as traffic_delays_minutes,
  null as mechanical_delays_minutes,
  null as performance_score
from s
join d on s.shipment_date = d.date
group by route_id, d.date_key

