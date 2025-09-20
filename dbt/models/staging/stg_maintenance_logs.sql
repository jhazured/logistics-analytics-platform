with src as (
  select * from {{ source('raw', 'azure_maintenance') }}
)
select
  try_to_number(maintenance_id) as maintenance_id,
  trim(vehicle_id) as vehicle_id,
  trim(maintenance_type) as maintenance_type,
  try_to_date(maintenance_date) as maintenance_date,
  try_to_decimal(odometer_reading, 12, 0) as maintenance_mileage,
  try_to_decimal(total_cost, 10, 2) as maintenance_cost_usd,
  try_to_decimal(parts_cost, 10, 2) as parts_cost,
  try_to_decimal(labor_cost, 10, 2) as labor_cost,
  trim(description) as description,
  trim(maintenance_provider) as service_provider,
  try_to_date(next_maintenance_due_date) as next_maintenance_due_date,
  try_to_decimal(next_maintenance_due_mileage, 12, 0) as next_maintenance_due_mileage,
  trim(maintenance_status) as maintenance_status,
  current_timestamp() as _ingested_at
from src

