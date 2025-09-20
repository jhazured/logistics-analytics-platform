{{ config(
    materialized='view',
    tags=['staging', 'vehicles']
) }}

with src as (
  select * from {{ source('raw_logistics', 'VEHICLES') }}
)
select
  cast("vehicle_id" as varchar) as vehicle_id,
  coalesce(trim("vehicle_type"), 'UNKNOWN') as vehicle_type,
  "capacity_lbs" / 2.20462 as capacity_kg,  -- Convert lbs to kg
  "capacity_cubic_feet" / 35.3147 as capacity_m3,  -- Convert cubic feet to m3
  "fuel_efficiency_mpg" as fuel_efficiency_mpg,  -- Keep as mpg
  coalesce(trim("make"), 'UNKNOWN') as make,
  coalesce(trim("model"), 'UNKNOWN') as model,
  try_to_number("model_year") as model_year,
  coalesce(trim("vehicle_status"), 'ACTIVE') as vehicle_status,  -- Use vehicle_status field
  "current_mileage" as current_mileage,
  to_date(to_timestamp_ntz("last_maintenance_date", 9)) as last_maintenance_date,  -- Convert nanoseconds to date
  to_date(to_timestamp_ntz("next_maintenance_date", 9)) as next_maintenance_date,  -- Convert nanoseconds to date
  "maintenance_interval_miles" as maintenance_interval_miles,
  "purchase_price" as purchase_price,
  "current_value" as current_value,
  current_timestamp() as _ingested_at
from src
