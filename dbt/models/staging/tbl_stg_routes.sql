{{ config(
    materialized='view',
    tags=['staging', 'routes']
) }}

with src as (
  select
    "origin_location_id",
    "destination_location_id",
    "distance_miles" * 1.60934 as distance_km  -- Convert miles to km
  from {{ source('raw_logistics', 'SHIPMENTS') }}
  where "origin_location_id" is not null and "destination_location_id" is not null
)
select distinct
  cast(concat("origin_location_id", "destination_location_id") as number) as route_id,
  cast("origin_location_id" as number) as origin_location_id,
  cast("destination_location_id" as number) as destination_location_id,
  distance_km
from src
