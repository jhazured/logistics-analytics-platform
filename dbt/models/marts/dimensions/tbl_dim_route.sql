{{ config(
    materialized='table',
    tags=['marts', 'dimensions', 'route', 'load_first']
) }}

with r as (
  select * from {{ ref('tbl_stg_shipments') }}
)
select
  concat(origin_location_id, '_', destination_location_id) as route_id,
  concat(origin_location_id, '_', destination_location_id) as route_sk,
  origin_location_id,
  destination_location_id,
  distance_km
from r
