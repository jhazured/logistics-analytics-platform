{{ config(
    materialized='table',
    tags=['marts', 'facts', 'route_conditions', 'load_second']
) }}

with s as (
  select route_id, shipment_date, origin_location_id from {{ ref('tbl_fact_shipments') }}
), d as (
  select date_key, date from {{ ref('tbl_dim_date') }}
), w as (
  select date, location_id, weather_id, weather_condition from {{ ref('tbl_raw_weather_data') }}
), t as (
  select date, location_id, traffic_id, traffic_level from {{ ref('tbl_raw_traffic_data') }}
), r as (
  select s.origin_location_id, s.destination_location_id, l.location_id
  from {{ ref('tbl_raw_azure_shipments') }} s
  left join {{ ref('tbl_dim_location') }} l on s.origin_location_id = l.location_id
)
select
  {{ dbt_utils.generate_surrogate_key(['s.route_id','d.date_key']) }} as condition_id,
  s.route_id,
  d.date_key,
  w.weather_id,
  t.traffic_id,
  round((coalesce(w.driving_impact_score,5) + coalesce(t.congestion_score,5)) / 2, 1) as road_condition_score,
  coalesce(w.driving_impact_score, 5) as weather_impact_score,
  coalesce(t.congestion_score, 5) as traffic_impact_score,
  round((coalesce(w.driving_impact_score,5) + coalesce(t.congestion_score,5)) / 2, 1) as overall_difficulty_score,
  null as recommended_vehicle_types,
  null as estimated_delay_minutes
from s
join d on s.shipment_date = d.date
left join r on r.route_id = s.route_id
left join w on w.date = d.date and w.city = r.city
left join t on t.date = d.date and t.city = r.city

