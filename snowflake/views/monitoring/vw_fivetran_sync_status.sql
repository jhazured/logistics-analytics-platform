-- 16. Fivetran Sync Status
-- File: models/analytics/monitoring/view_fivetran_sync_status.sql
{{ config(
    materialized='view',
    tags=['monitoring', 'fivetran', 'sync']
) }}

-- Note: This would typically pull from Fivetran's metadata tables
-- For demo purposes, we'll simulate sync status based on data patterns

WITH sync_simulation AS (
    SELECT 
        'shipments_db' AS connector_name,
        'SQL Server' AS source_type,
        'fact_shipments' AS table_name,
        MAX(updated_at) AS last_sync_time,
        COUNT(*) AS records_synced,
        CASE 
            WHEN MAX(updated_at) >= CURRENT_TIMESTAMP() - INTERVAL '1 hour' THEN 'SUCCESS'
            WHEN MAX(updated_at) >= CURRENT_TIMESTAMP() - INTERVAL '6 hours' THEN 'WARNING'
            ELSE 'ERROR'
        END AS sync_status,
        '*/15 * * * *' AS sync_frequency,  -- Every 15 minutes
        DATEDIFF(minute, MAX(updated_at), CURRENT_TIMESTAMP()) AS minutes_since_sync
    FROM {{ ref('fact_shipments') }}
    
    UNION ALL
    
    SELECT 
        'vehicle_telematics' AS connector_name,
        'API' AS source_type,
        'fact_vehicle_telemetry' AS table_name,
        MAX(created_at) AS last_sync_time,
        COUNT(*) AS records_synced,
        CASE 
            WHEN MAX(created_at) >= CURRENT_TIMESTAMP() - INTERVAL '5 minutes' THEN 'SUCCESS'
            WHEN MAX(created_at) >= CURRENT_TIMESTAMP() - INTERVAL '30 minutes' THEN 'WARNING'
            ELSE 'ERROR'
        END AS sync_status,
        '*/5 * * * *' AS sync_frequency,  -- Every 5 minutes
        DATEDIFF(minute, MAX(created_at), CURRENT_TIMESTAMP()) AS minutes_since_sync
    FROM {{ ref('fact_vehicle_telemetry') }}
    
    UNION ALL
    
    SELECT 
        'weather_api' AS connector_name,
        'API' AS source_type,
        'dim_weather' AS table_name,
        MAX(created_at) AS last_sync_time,
        COUNT(*) AS records_synced,
        CASE 
            WHEN MAX(created_at) >= CURRENT_TIMESTAMP() - INTERVAL '1 hour' THEN 'SUCCESS'
            WHEN MAX(created_at) >= CURRENT_TIMESTAMP() - INTERVAL '3 hours' THEN 'WARNING'
            ELSE 'ERROR'
        END AS sync_status,
        '0 * * * *' AS sync_frequency,  -- Hourly
        DATEDIFF(minute, MAX(created_at), CURRENT_TIMESTAMP()) AS minutes_since_sync
    FROM {{ ref('dim_weather') }}
)

SELECT 
    connector_name,
    source_type,
    table_name,
    last_sync_time,
    records_synced,
    sync_status,
    sync_frequency,
    minutes_since_sync,
    
    -- Sync health metrics
    CASE 
        WHEN sync_status = 'SUCCESS' THEN 100
        WHEN sync_status = 'WARNING' THEN 70
        ELSE 0
    END AS sync_health_score,
    
    -- Cost optimization insights
    CASE 
        WHEN minutes_since_sync < 5 AND sync_frequency = '*/5 * * * *' THEN 'optimal'
        WHEN minutes_since_sync < 15 AND sync_frequency = '*/15 * * * *' THEN 'optimal'
        WHEN minutes_since_sync < 60 AND sync_frequency = '0 * * * *' THEN 'optimal'
        WHEN minutes_since_sync > 120 THEN 'underperforming'
        ELSE 'review_frequency'
    END AS optimization_recommendation,
    
    -- Data volume validation
    CASE 
        WHEN records_synced = 0 THEN 'no_data'
        WHEN connector_name = 'vehicle_telematics' AND records_synced < 1000 THEN 'low_volume'
        WHEN connector_name = 'shipments_db' AND records_synced < 100 THEN 'low_volume'
        ELSE 'normal_volume'
    END AS volume_status,
    
    CURRENT_TIMESTAMP() AS status_check_time

FROM sync_simulation