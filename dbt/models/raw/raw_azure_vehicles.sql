-- Raw vehicle data from Azure source system
-- This model extracts and standardizes vehicle data from Azure

{{ config(
    materialized='view',
    tags=['raw', 'vehicles', 'azure']
) }}

SELECT 
    vehicle_id,
    vehicle_number,
    vehicle_type,
    make,
    model,
    model_year,
    capacity_lbs,
    capacity_cubic_feet,
    fuel_type,
    fuel_efficiency_mpg,
    maintenance_interval_miles,
    current_mileage,
    last_maintenance_date,
    next_maintenance_date,
    vehicle_status,
    assigned_driver_id,
    insurance_expiry,
    registration_expiry,
    purchase_date,
    purchase_price,
    current_value,
    created_at,
    updated_at,
    _loaded_at
FROM {{ source('raw_logistics', 'vehicles') }}
WHERE vehicle_status IN ('ACTIVE', 'MAINTENANCE')
