{{ config(
    materialized='view',
    tags=['staging', 'traffic']
) }}

with src as (
  select * from {{ source('raw_logistics', 'TRAFFIC') }}
)
select
  try_to_number("traffic_id") as traffic_id,
  try_to_date("date") as date,
  try_to_number("hour") as hour,
  try_to_number("location_id") as location_id,
  trim("traffic_level") as traffic_level,
  "congestion_delay_minutes" as congestion_delay_minutes,
  "average_speed_mph" * 1.60934 as average_speed_kmh,  -- Convert mph to kmh
  "free_flow_speed_mph" * 1.60934 as free_flow_speed_kmh,  -- Convert mph to kmh
  "travel_time_minutes" as travel_time_minutes,
  "free_flow_travel_time_minutes" as free_flow_travel_time_minutes,
  "confidence_score" as confidence_score,
  trim("road_type") as road_type,
  try_to_number("incident_count") as incident_count,
  trim("weather_impact") as weather_impact,
  current_timestamp() as _ingested_at
from src

