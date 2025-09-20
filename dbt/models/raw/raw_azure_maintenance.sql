-- Raw maintenance data from Azure source system
-- This model extracts and standardizes maintenance data from Azure

{{ config(
    materialized='view',
    tags=['raw', 'maintenance', 'azure']
) }}

SELECT 
    maintenance_id,
    vehicle_id,
    maintenance_type,
    maintenance_date,
    odometer_reading,
    description,
    parts_cost,
    labor_cost,
    total_cost,
    maintenance_provider,
    next_maintenance_due_date,
    next_maintenance_due_mileage,
    maintenance_status,
    created_at,
    updated_at,
    _loaded_at
FROM {{ source('raw_logistics', 'maintenance') }}
WHERE maintenance_status = 'COMPLETED'
