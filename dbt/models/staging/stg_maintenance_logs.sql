with src as (
  select * from {{ source('raw', 'azure_maintenance') }}
)
select
  try_to_number(maintenance_id) as maintenance_id,
  trim(vehicle_id) as vehicle_id,
  trim(maintenance_type) as maintenance_type,
  try_to_date(scheduled_date) as scheduled_date,
  try_to_date(completed_date) as completed_date,
  try_to_decimal(cost, 10, 2) as cost,
  trim(description) as description,
  trim(service_provider) as service_provider,
  try_to_number(next_service_km) as next_service_km,
  trim(priority_level) as priority_level,
  trim(status) as status
from src

