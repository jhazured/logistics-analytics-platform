-- Raw vehicle data from Azure source system
-- This model extracts and standardizes vehicle data from Azure
-- Uses incremental loading to minimize Fivetran costs

{{ config(
    materialized='incremental',
    unique_key='vehicle_id',
    merge_update_columns=['vehicle_number', 'vehicle_type', 'make', 'model', 'model_year', 'capacity_lbs', 'capacity_cubic_feet', 'fuel_type', 'fuel_efficiency_mpg', 'maintenance_interval_miles', 'current_mileage', 'last_maintenance_date', 'next_maintenance_date', 'vehicle_status', 'assigned_driver_id', 'insurance_expiry', 'registration_expiry', 'purchase_date', 'purchase_price', 'current_value', 'created_at', 'updated_at', '_loaded_at'],
    tags=['raw', 'vehicles', 'azure', 'incremental']
) }}

SELECT 
    "vehicle_id",
    "vehicle_number",
    "vehicle_type",
    "make",
    "model",
    "model_year",
    "capacity_lbs",
    "capacity_cubic_feet",
    "fuel_type",
    "fuel_efficiency_mpg",
    "maintenance_interval_miles",
    "current_mileage",
    "last_maintenance_date",
    "next_maintenance_date",
    "vehicle_status",
    "assigned_driver_id",
    "insurance_expiry",
    "registration_expiry",
    "purchase_date",
    "purchase_price",
    "current_value",
    "created_at",
    "updated_at",
    "_loaded_at"
FROM {{ source('raw_logistics', 'VEHICLES') }}
WHERE "vehicle_status" IN ('ACTIVE', 'MAINTENANCE')

{% if is_incremental() %}
    -- Only process records that are new or updated since last run
    AND "_loaded_at" > (SELECT MAX("_loaded_at") FROM {{ this }})
{% endif %}
