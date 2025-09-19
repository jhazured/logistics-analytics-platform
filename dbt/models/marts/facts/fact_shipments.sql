with s as (
  select * from {{ ref(stg_shipments) }}
)
select
  shipment_id,
  {{ dbt_utils.generate_surrogate_key([shipment_id]) }} as shipment_sk,
  date_trunc(day, shipment_date) as shipment_date,
  to_number(to_char(shipment_date, YYYYMMDD)) as date_key,
  customer_id,
  origin_location_id,
  destination_location_id,
  vehicle_id,
  route_id,
  weight_kg,
  volume_m3,
  distance_km,
  planned_duration_minutes,
  actual_duration_minutes,
  fuel_cost,
  delivery_cost,
  revenue,
  (actual_delivery_date <= planned_delivery_date) as is_on_time,
  (delivery_status = 'Delivered') as is_delivered,
  delivery_status,
  priority_level,
  service_type
from s
