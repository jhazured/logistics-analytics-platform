with v as (
  select * from {{ ref('tbl_stg_vehicles') }}
)
select
  "vehicle_id",
  {{ dbt_utils.generate_surrogate_key(['"vehicle_id"']) }} as vehicle_sk,
  "vehicle_type",
  "capacity_lbs" / 2.20462 as capacity_kg,  -- Convert lbs to kg
  "capacity_cubic_feet" / 35.3147 as capacity_m3,  -- Convert cubic feet to m3
  "fuel_efficiency_mpg",
  "make",
  "model",
  "model_year",
  "vehicle_status",
  "current_mileage",
  to_date("last_maintenance_date") as last_maintenance_date,  -- Convert Unix timestamp to date
  to_date("next_maintenance_date") as next_maintenance_date,  -- Convert Unix timestamp to date
  "maintenance_interval_miles",
  "purchase_price",
  "current_value"
from v
