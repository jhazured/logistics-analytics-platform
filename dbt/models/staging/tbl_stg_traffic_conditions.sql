with src as (
  select * from {{ source('raw_logistics', 'traffic') }}
)
select
  try_to_number(traffic_id) as traffic_id,
  try_to_date(date) as date,
  try_to_number(hour) as hour,
  try_to_number(location_id) as location_id,
  trim(traffic_level) as traffic_level,
  try_to_decimal(congestion_delay_minutes, 6, 1) as congestion_delay_minutes,
  try_to_decimal(average_speed_mph, 6, 1) * 1.60934 as average_speed_kmh,  -- Convert mph to kmh
  try_to_decimal(free_flow_speed_mph, 6, 1) * 1.60934 as free_flow_speed_kmh,  -- Convert mph to kmh
  try_to_decimal(travel_time_minutes, 6, 1) as travel_time_minutes,
  try_to_decimal(free_flow_travel_time_minutes, 6, 1) as free_flow_travel_time_minutes,
  try_to_decimal(confidence_score, 4, 2) as confidence_score,
  trim(road_type) as road_type,
  try_to_number(incident_count) as incident_count,
  trim(weather_impact) as weather_impact,
  current_timestamp() as _ingested_at
from src

