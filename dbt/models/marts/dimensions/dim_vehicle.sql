with v as (
  select * from {{ ref('stg_vehicles') }}
)
select
  vehicle_id,
  {{ dbt_utils.generate_surrogate_key(['vehicle_id']) }} as vehicle_sk,
  vehicle_type,
  capacity_kg,
  capacity_m3,
  fuel_efficiency_km_per_l,
  make,
  model,
  model_year,
  status
from v
