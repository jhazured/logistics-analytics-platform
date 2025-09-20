{{ config(
    materialized='incremental',
    unique_key='shipment_sk',
    on_schema_change='sync_all_columns',
    tags=['marts', 'facts', 'shipments', 'load_second']
) }}

with s as (
  select * from {{ ref('tbl_stg_shipments') }}
  {% if is_incremental() %}
    where shipment_date > (select coalesce(max(shipment_date), '1900-01-01') from {{ this }})
  {% endif %}
),

-- Join with dimensions for calculated fields
shipments_with_dims as (
  select 
    s.*,
    dv.vehicle_type,
    dv.fuel_efficiency_mpg,
    dv.make,
    dv.model,
    dv.vehicle_status
  from s
  left join {{ ref('tbl_dim_vehicle') }} dv on s.vehicle_id = dv.vehicle_id
)

select
  shipment_id,
  shipment_id as shipment_sk,
  date_trunc('day', shipment_date) as shipment_date,
  null as date_key,
  customer_id,
  origin_location_id,
  destination_location_id,
  vehicle_id,
  route_id,
  
  -- Basic fields
  weight_kg,
  volume_m3,
  distance_km,
  planned_duration_minutes,
  actual_duration_minutes,
  fuel_cost,
  delivery_cost,
  revenue,
  
  -- Calculated distance fields (convert km to miles)
  distance_km * 0.621371 as actual_distance_miles,
  distance_km * 0.621371 as planned_distance_miles,
  
  -- Calculated time fields (convert minutes to hours)
  actual_duration_minutes / 60.0 as actual_delivery_time_hours,
  planned_duration_minutes / 60.0 as estimated_delivery_time_hours,
  
  -- Calculated cost fields (convert to USD)
  fuel_cost as fuel_cost_usd,
  delivery_cost as driver_cost_usd,
  fuel_cost + delivery_cost as total_cost_usd,
  
  -- Calculated financial metrics
  revenue - fuel_cost - delivery_cost as profit_usd,
  case 
    when revenue > 0 then (revenue - fuel_cost - delivery_cost) / revenue * 100
    else 0
  end as profit_margin_pct,
  
  -- Performance indicators
  (actual_delivery_date <= planned_delivery_date) as is_on_time,
  (delivery_status = 'Delivered') as is_delivered,
  case 
    when actual_delivery_date <= planned_delivery_date then 1
    else 0
  end as on_time_delivery_flag,
  
  -- Route efficiency score (0-100)
  case 
    when planned_duration_minutes > 0 and actual_duration_minutes > 0 then
      greatest(0, least(100, 100 - ((actual_duration_minutes - planned_duration_minutes) / planned_duration_minutes * 100)))
    else 50
  end as route_efficiency_score,
  
  -- Carbon emissions calculation (kg CO2)
  case 
    when distance_km > 0 and fuel_efficiency_mpg > 0 then
      (distance_km * 0.621371) / fuel_efficiency_mpg * 2.31 * 8.887
    else 0
  end as carbon_emissions_kg,
  
  -- Delay calculations (placeholder - would need actual delay data)
  case 
    when actual_duration_minutes > planned_duration_minutes then
      actual_duration_minutes - planned_duration_minutes
    else 0
  end as weather_delay_minutes,
  
  case 
    when actual_duration_minutes > planned_duration_minutes then
      (actual_duration_minutes - planned_duration_minutes) * 0.3
    else 0
  end as traffic_delay_minutes,
  
  -- Status fields
  delivery_status,
  priority_level,
  service_type,
  
  -- Dimension context fields
  vehicle_type,
  fuel_efficiency_mpg,
  make,
  model,
  vehicle_status
from shipments_with_dims
