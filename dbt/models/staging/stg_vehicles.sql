with src as (
  select * from {{ source(raw, azure_vehicles) }}
)
select
  cast(vehicle_id as varchar) as vehicle_id,
  coalesce(trim(vehicle_type), UNKNOWN) as vehicle_type,
  try_to_decimal(capacity_kg, 12, 2) as capacity_kg,
  try_to_decimal(capacity_m3, 12, 3) as capacity_m3,
  try_to_decimal(fuel_efficiency_km_per_l, 10, 3) as fuel_efficiency_km_per_l,
  coalesce(trim(make), UNKNOWN) as make,
  coalesce(trim(model), UNKNOWN) as model,
  try_to_number(model_year) as model_year,
  coalesce(trim(status), ACTIVE) as status,
  current_timestamp() as _ingested_at
from src
