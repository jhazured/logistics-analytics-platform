-- Raw telematics data from vehicle tracking systems
-- This model extracts and standardizes telematics data
-- Uses incremental loading to minimize Fivetran costs

{{ config(
    materialized='incremental',
    unique_key='telemetry_id',
    merge_update_columns=['vehicle_id', 'timestamp', 'speed_mph', 'engine_rpm', 'fuel_level_pct', 'engine_temperature_f', 'brake_pressure_psi', 'tire_pressure_psi', 'latitude', 'longitude', 'altitude_ft', 'heading_degrees', 'acceleration_g', 'created_at', '_loaded_at'],
    tags=['raw', 'telematics', 'iot', 'incremental']
) }}

SELECT 
    "telemetry_id",
    "vehicle_id",
    "timestamp",
    "speed_mph",
    "engine_rpm",
    "fuel_level_pct",
    "engine_temperature_f",
    "brake_pressure_psi",
    "tire_pressure_psi",
    "latitude",
    "longitude",
    "altitude_ft",
    "heading_degrees",
    "acceleration_g",
    "created_at",
    "_loaded_at"
FROM {{ source('raw_logistics', 'TELEMATICS') }}
WHERE "timestamp" >= (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP()) - 604800)

{% if is_incremental() %}
    -- Only process records that are new or updated since last run
    AND "_loaded_at" > (SELECT MAX("_loaded_at") FROM {{ this }})
{% endif %}
