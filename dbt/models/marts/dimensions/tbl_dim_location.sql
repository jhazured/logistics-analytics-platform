with origins as (
  select distinct origin_location_id as location_id
  from {{ ref('tbl_stg_routes') }}
),
destinations as (
  select distinct destination_location_id as location_id
  from {{ ref('tbl_stg_routes') }}
),
all_locations as (
  select location_id from origins
  union
  select location_id from destinations
)
select
  location_id
from all_locations

