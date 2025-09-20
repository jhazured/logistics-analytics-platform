-- Raw traffic data from external API
-- This model extracts and standardizes traffic data

{{ config(
    materialized='view',
    tags=['raw', 'traffic', 'external']
) }}

SELECT 
    traffic_id,
    location_id,
    date,
    hour,
    traffic_level,
    congestion_delay_minutes,
    average_speed_mph,
    free_flow_speed_mph,
    travel_time_minutes,
    free_flow_travel_time_minutes,
    confidence_score,
    road_type,
    incident_count,
    weather_impact,
    created_at,
    _loaded_at
FROM {{ source('raw_logistics', 'traffic') }}
WHERE date >= DATEADD('day', -30, CURRENT_DATE())
