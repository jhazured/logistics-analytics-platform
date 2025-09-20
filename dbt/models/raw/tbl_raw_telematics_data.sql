-- Raw telematics data from vehicle tracking systems
-- This model extracts and standardizes telematics data
-- Uses incremental loading to minimize Fivetran costs

{{ config(
    materialized='incremental',
    unique_key='telemetry_id',
    merge_update_columns=['vehicle_id', 'timestamp', 'latitude', 'longitude', 'speed_mph', 'heading_degrees', 'engine_rpm', 'fuel_level_pct', 'engine_temperature_f', 'battery_voltage', 'odometer_miles', 'accelerometer_x', 'accelerometer_y', 'accelerometer_z', 'gyroscope_x', 'gyroscope_y', 'gyroscope_z', 'brake_pressure', 'throttle_position', 'gear_position', 'created_at', '_loaded_at'],
    tags=['raw', 'telematics', 'iot', 'incremental']
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

{% if is_incremental() %}
    -- Only process records that are new or updated since last run
    AND _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}
