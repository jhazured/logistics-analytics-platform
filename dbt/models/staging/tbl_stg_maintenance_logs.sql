{{ config(
    materialized='view',
    tags=['staging', 'maintenance']
) }}

with src as (
  select * from {{ source('raw_logistics', 'MAINTENANCE') }}
)
select
  try_to_number("maintenance_id") as maintenance_id,
  trim("vehicle_id") as vehicle_id,
  trim("maintenance_type") as maintenance_type,
  to_date(to_timestamp_ntz("maintenance_date", 9)) as maintenance_date,  -- Convert nanoseconds to date
  "odometer_reading" as maintenance_mileage,
  "total_cost" as maintenance_cost_usd,
  "parts_cost" as parts_cost,
  "labor_cost" as labor_cost,
  trim("description") as description,
  trim("maintenance_provider") as service_provider,
  to_date(to_timestamp_ntz("next_maintenance_due_date", 9)) as next_maintenance_due_date,  -- Convert nanoseconds to date
  "next_maintenance_due_mileage" as next_maintenance_due_mileage,
  trim("maintenance_status") as maintenance_status,
  current_timestamp() as _ingested_at
from src

