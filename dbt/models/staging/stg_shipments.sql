with src as (
  select * from {{ source(raw, azure_shipments) }}
)
select
  cast(shipment_id as number) as shipment_id,
  to_date(shipment_date) as shipment_date,
  cast(customer_id as number) as customer_id,
  cast(origin_location_id as number) as origin_location_id,
  cast(destination_location_id as number) as destination_location_id,
  cast(vehicle_id as varchar) as vehicle_id,
  cast(route_id as number) as route_id,
  try_to_decimal(weight_kg, 10, 1) as weight_kg,
  try_to_decimal(volume_m3, 8, 2) as volume_m3,
  try_to_decimal(distance_km, 8, 1) as distance_km,
  try_to_number(planned_duration_minutes) as planned_duration_minutes,
  try_to_number(actual_duration_minutes) as actual_duration_minutes,
  try_to_decimal(fuel_cost, 10, 2) as fuel_cost,
  try_to_decimal(delivery_cost, 10, 2) as delivery_cost,
  try_to_decimal(revenue, 10, 2) as revenue,
  planned_delivery_date::date as planned_delivery_date,
  actual_delivery_date::date as actual_delivery_date,
  delivery_status,
  priority_level,
  service_type,
  current_timestamp() as _ingested_at
from src
