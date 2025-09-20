{{ config(
    materialized='view',
    tags=['staging', 'shipments']
) }}

with src as (
  select * from {{ source('raw_logistics', 'SHIPMENTS') }}
)
select
  cast("shipment_id" as number) as shipment_id,
  to_date(to_timestamp_ntz("pickup_date", 9)) as shipment_date,  -- Convert nanoseconds to date
  cast("customer_id" as number) as customer_id,
  cast("origin_location_id" as number) as origin_location_id,
  cast("destination_location_id" as number) as destination_location_id,
  cast("vehicle_id" as varchar) as vehicle_id,
  -- Generate route_id based on origin/destination (since raw doesn't have it)
  cast(concat("origin_location_id", "destination_location_id") as number) as route_id,
  "weight_lbs" / 2.20462 as weight_kg,  -- Convert lbs to kg
  "volume_cubic_feet" / 35.3147 as volume_m3,  -- Convert cubic feet to m3
  "distance_miles" * 1.60934 as distance_km,  -- Convert miles to km
  -- Calculate planned duration based on distance (since raw doesn't have it)
  "distance_miles" * 2.5 as planned_duration_minutes,  -- Rough estimate: 2.5 min/mile
  "delivery_time_hours" * 60 as actual_duration_minutes,  -- Convert hours to minutes
  "fuel_cost" as fuel_cost,
  "driver_cost" as delivery_cost,  -- Map driver_cost to delivery_cost
  "revenue" as revenue,
  to_date(to_timestamp_ntz("requested_delivery_date", 9)) as planned_delivery_date,  -- Convert nanoseconds to date
  to_date(to_timestamp_ntz("actual_delivery_date", 9)) as actual_delivery_date,  -- Convert nanoseconds to date
  coalesce(trim("shipment_status"), 'PENDING') as delivery_status,  -- Map shipment_status to delivery_status
  -- Generate priority_level based on revenue (since raw doesn't have it)
  case 
    when "revenue" > 1000 then 'HIGH'
    when "revenue" > 500 then 'MEDIUM'
    else 'LOW'
  end as priority_level,
  -- Generate service_type based on delivery time (since raw doesn't have it)
  case 
    when "delivery_time_hours" < 2 then 'EXPRESS'
    when "delivery_time_hours" < 8 then 'STANDARD'
    else 'ECONOMY'
  end as service_type,
  current_timestamp() as _ingested_at
from src
