-- Raw shipment data from Azure source system
-- This model extracts and standardizes shipment data from Azure

{{ config(
    materialized='view',
    tags=['raw', 'shipments', 'azure']
) }}

SELECT 
    shipment_id,
    customer_id,
    vehicle_id,
    driver_id,
    origin_location_id,
    destination_location_id,
    pickup_date,
    delivery_date,
    requested_delivery_date,
    actual_delivery_date,
    shipment_status,
    weight_lbs,
    volume_cubic_feet,
    shipment_value,
    fuel_cost,
    driver_cost,
    total_cost,
    revenue,
    distance_miles,
    delivery_time_hours,
    on_time_delivery,
    weather_conditions,
    traffic_conditions,
    special_instructions,
    created_at,
    updated_at,
    _loaded_at
FROM {{ source('raw_logistics', 'shipments') }}
WHERE shipment_status NOT IN ('CANCELLED', 'VOID')
