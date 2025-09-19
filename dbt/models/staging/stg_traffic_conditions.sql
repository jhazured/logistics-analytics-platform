with src as (
  select * from {{ source('raw', 'traffic_data') }}
)
select
  try_to_number(traffic_id) as traffic_id,
  try_to_date(date) as date,
  try_to_number(hour) as hour,
  trim(city) as city,
  trim(traffic_level) as traffic_level,
  try_to_decimal(congestion_score, 4, 1) as congestion_score,
  try_to_decimal(average_speed_kmh, 6, 1) as average_speed_kmh,
  try_to_number(incident_count) as incident_count,
  try_to_number(road_closures) as road_closures
from src

