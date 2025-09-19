with s as (
  select route_id, shipment_date, origin_location_id from {{ ref('fact_shipments') }}
), d as (
  select date_key, date from {{ ref('dim_date') }}
), w as (
  select date, city, weather_id, driving_impact_score from {{ ref('stg_weather_conditions') }}
), t as (
  select date, city, traffic_id, congestion_score from {{ ref('stg_traffic_conditions') }}
), r as (
  select r.route_id, r.origin_location_id, l.city
  from {{ ref('stg_routes') }} r
  left join {{ ref('dim_location') }} l on r.origin_location_id = l.location_id
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

