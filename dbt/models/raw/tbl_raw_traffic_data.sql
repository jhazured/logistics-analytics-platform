-- Raw traffic data from external API
-- This model extracts and standardizes traffic data
-- Uses incremental loading to minimize Fivetran costs

{{ config(
    materialized='incremental',
    unique_key='"traffic_id"',
    merge_update_columns=['"location_id"', '"date"', '"hour"', '"traffic_level"', '"average_speed_mph"', '"congestion_delay_minutes"', '"incident_count"', '"road_type"', '"weather_impact"', '"created_at"', '"_loaded_at"'],
    tags=['raw', 'traffic', 'external', 'incremental']
) }}

SELECT 
    "traffic_id",
    "location_id",
    "date",
    "hour",
    "traffic_level",
    "congestion_delay_minutes",
    "average_speed_mph",
    "free_flow_speed_mph",
    "travel_time_minutes",
    "free_flow_travel_time_minutes",
    "confidence_score",
    "road_type",
    "incident_count",
    "weather_impact",
    "created_at",
    "_loaded_at"
FROM {{ source('raw_logistics', 'TRAFFIC') }}
WHERE "date" >= DATEADD('day', -30, CURRENT_DATE())

{% if is_incremental() %}
    -- Only process records that are new or updated since last run
    AND "_loaded_at" > (SELECT MAX("_loaded_at") FROM {{ this }})
{% endif %}
