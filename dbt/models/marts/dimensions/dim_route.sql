with r as (
  select * from {{ ref('stg_routes') }}
)
select
  route_id,
  {{ dbt_utils.generate_surrogate_key(['route_id']) }} as route_sk,
  origin_location_id,
  destination_location_id,
  distance_km
from r
