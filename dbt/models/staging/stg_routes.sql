with src as (
  select
    route_id,
    origin_location_id,
    destination_location_id,
    try_to_decimal(distance_km, 8, 1) as distance_km
  from {{ source(raw, azure_shipments) }}
  where route_id is not null
)
select distinct
  cast(route_id as number) as route_id,
  cast(origin_location_id as number) as origin_location_id,
  cast(destination_location_id as number) as destination_location_id,
  distance_km
from src
