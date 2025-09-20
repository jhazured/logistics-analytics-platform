-- Raw maintenance data from Azure source system
-- This model extracts and standardizes maintenance data from Azure
-- Uses incremental loading to minimize Fivetran costs

{{ config(
    materialized='incremental',
    unique_key='"maintenance_id"',
    merge_update_columns=['"vehicle_id"', '"maintenance_type"', '"maintenance_date"', '"odometer_reading"', '"description"', '"parts_cost"', '"labor_cost"', '"total_cost"', '"maintenance_provider"', '"next_maintenance_due_date"', '"next_maintenance_due_mileage"', '"maintenance_status"', '"created_at"', '"updated_at"', '"_loaded_at"'],
    tags=['raw', 'maintenance', 'azure', 'incremental']
) }}

SELECT 
    "maintenance_id",
    "vehicle_id",
    "maintenance_type",
    "maintenance_date",
    "odometer_reading",
    "description",
    "parts_cost",
    "labor_cost",
    "total_cost",
    "maintenance_provider",
    "next_maintenance_due_date",
    "next_maintenance_due_mileage",
    "maintenance_status",
    "created_at",
    "updated_at",
    "_loaded_at"
FROM {{ source('raw_logistics', 'MAINTENANCE') }}
WHERE "maintenance_status" = 'COMPLETED'

{% if is_incremental() %}
    -- Only process records that are new or updated since last run
    AND "_loaded_at" > (SELECT MAX("_loaded_at") FROM {{ this }})
{% endif %}
