with v as (
  select * from {{ ref('tbl_stg_vehicles') }}
)
select
  vehicle_id,
  {{ dbt_utils.generate_surrogate_key(['vehicle_id']) }} as vehicle_sk,
  vehicle_type,
  capacity_kg,
  capacity_m3,
  fuel_efficiency_mpg,  -- Use mpg from staging
  make,
  model,
  model_year,
  vehicle_status,  -- Use vehicle_status from staging
  current_mileage,
  last_maintenance_date,
  next_maintenance_date,
  maintenance_interval_miles,
  purchase_price,
  current_value
from v
