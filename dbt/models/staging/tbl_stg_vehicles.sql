with src as (
  select * from {{ source('raw_logistics', 'vehicles') }}
)
select
  cast(vehicle_id as varchar) as vehicle_id,
  coalesce(trim(vehicle_type), 'UNKNOWN') as vehicle_type,
  try_to_decimal(capacity_lbs, 12, 2) / 2.20462 as capacity_kg,  -- Convert lbs to kg
  try_to_decimal(capacity_cubic_feet, 12, 3) / 35.3147 as capacity_m3,  -- Convert cubic feet to m3
  try_to_decimal(fuel_efficiency_mpg, 10, 3) as fuel_efficiency_mpg,  -- Keep as mpg
  coalesce(trim(make), 'UNKNOWN') as make,
  coalesce(trim(model), 'UNKNOWN') as model,
  try_to_number(model_year) as model_year,
  coalesce(trim(vehicle_status), 'ACTIVE') as vehicle_status,  -- Use vehicle_status field
  try_to_decimal(current_mileage, 12, 0) as current_mileage,
  try_to_date(last_maintenance_date) as last_maintenance_date,
  try_to_date(next_maintenance_date) as next_maintenance_date,
  try_to_decimal(maintenance_interval_miles, 10, 0) as maintenance_interval_miles,
  try_to_decimal(purchase_price, 15, 2) as purchase_price,
  try_to_decimal(current_value, 15, 2) as current_value,
  current_timestamp() as _ingested_at
from src
