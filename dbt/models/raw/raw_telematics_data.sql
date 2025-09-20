-- Raw telematics data from vehicle tracking systems
-- This model extracts and standardizes telematics data

{{ config(
    materialized='view',
    tags=['raw', 'telematics', 'iot']
) }}

SELECT 
    telemetry_id,
    vehicle_id,
    timestamp,
    latitude,
    longitude,
    speed_mph,
    heading_degrees,
    engine_rpm,
    fuel_level_pct,
    engine_temperature_f,
    battery_voltage,
    odometer_miles,
    acceleration_g,
    brake_force,
    steering_angle,
    gps_accuracy_meters,
    signal_strength,
    created_at,
    _loaded_at
FROM {{ source('raw_logistics', 'telematics') }}
WHERE timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
