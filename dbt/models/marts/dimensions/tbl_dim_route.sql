{{ config(
    materialized='table',
    tags=['marts', 'dimensions', 'route', 'load_first']
) }}

with r as (
  select * from {{ ref('tbl_raw_azure_shipments') }}
)
select
  {{ dbt_utils.generate_surrogate_key(['"origin_location_id"', '"destination_location_id"']) }} as route_id,
  {{ dbt_utils.generate_surrogate_key(['"origin_location_id"', '"destination_location_id"']) }} as route_sk,
  "origin_location_id",
  "destination_location_id",
  "distance_miles" * 1.60934 as distance_km  -- Convert miles to km
from r
