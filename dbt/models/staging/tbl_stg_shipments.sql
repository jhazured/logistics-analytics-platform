with src as (
  select * from {{ source('raw', 'tbl_raw_azure_shipments') }}
)
select
  cast(shipment_id as number) as shipment_id,
  to_date(pickup_date) as shipment_date,  -- Use pickup_date from raw
  cast(customer_id as number) as customer_id,
  cast(origin_location_id as number) as origin_location_id,
  cast(destination_location_id as number) as destination_location_id,
  cast(vehicle_id as varchar) as vehicle_id,
  -- Generate route_id based on origin/destination (since raw doesn't have it)
  cast(concat(origin_location_id, destination_location_id) as number) as route_id,
  try_to_decimal(weight_lbs, 10, 1) / 2.20462 as weight_kg,  -- Convert lbs to kg
  try_to_decimal(volume_cubic_feet, 8, 2) / 35.3147 as volume_m3,  -- Convert cubic feet to m3
  try_to_decimal(distance_miles, 8, 1) * 1.60934 as distance_km,  -- Convert miles to km
  -- Calculate planned duration based on distance (since raw doesn't have it)
  try_to_number(distance_miles * 2.5) as planned_duration_minutes,  -- Rough estimate: 2.5 min/mile
  try_to_number(delivery_time_hours * 60) as actual_duration_minutes,  -- Convert hours to minutes
  try_to_decimal(fuel_cost, 10, 2) as fuel_cost,
  try_to_decimal(driver_cost, 10, 2) as delivery_cost,  -- Map driver_cost to delivery_cost
  try_to_decimal(revenue, 10, 2) as revenue,
  try_to_date(requested_delivery_date) as planned_delivery_date,  -- Use requested_delivery_date
  try_to_date(actual_delivery_date) as actual_delivery_date,
  coalesce(trim(shipment_status), 'PENDING') as delivery_status,  -- Map shipment_status to delivery_status
  -- Generate priority_level based on revenue (since raw doesn't have it)
  case 
    when try_to_decimal(revenue, 10, 2) > 1000 then 'HIGH'
    when try_to_decimal(revenue, 10, 2) > 500 then 'MEDIUM'
    else 'LOW'
  end as priority_level,
  -- Generate service_type based on delivery time (since raw doesn't have it)
  case 
    when try_to_number(delivery_time_hours) < 2 then 'EXPRESS'
    when try_to_number(delivery_time_hours) < 8 then 'STANDARD'
    else 'ECONOMY'
  end as service_type,
  current_timestamp() as _ingested_at
from src
