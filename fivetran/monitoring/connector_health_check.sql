-- Fivetran connector health check and monitoring
-- This file contains comprehensive health checks for all Fivetran connectors

-- 1. Overall connector health status
CREATE OR REPLACE VIEW fivetran_connector_health AS
SELECT 
    connector_name,
    connector_type,
    status,
    last_sync_time,
    sync_frequency_minutes,
    records_synced_24h,
    data_freshness_status,
    error_count_24h,
    overall_health_score,
    CASE 
        WHEN overall_health_score >= 90 THEN 'EXCELLENT'
        WHEN overall_health_score >= 80 THEN 'GOOD'
        WHEN overall_health_score >= 70 THEN 'FAIR'
        WHEN overall_health_score >= 60 THEN 'POOR'
        ELSE 'CRITICAL'
    END as health_status
FROM (
    SELECT 
        'azure_sql_connector' as connector_name,
        'azure_sql' as connector_type,
        'active' as status,
        MAX(updated_at) as last_sync_time,
        60 as sync_frequency_minutes,
        COUNT(*) as records_synced_24h,
        CASE 
            WHEN DATEDIFF(minute, MAX(updated_at), CURRENT_TIMESTAMP()) <= 120 THEN 'FRESH'
            WHEN DATEDIFF(minute, MAX(updated_at), CURRENT_TIMESTAMP()) <= 240 THEN 'STALE'
            ELSE 'CRITICAL'
        END as data_freshness_status,
        0 as error_count_24h,
        CASE 
            WHEN DATEDIFF(minute, MAX(updated_at), CURRENT_TIMESTAMP()) <= 120 THEN 100
            WHEN DATEDIFF(minute, MAX(updated_at), CURRENT_TIMESTAMP()) <= 240 THEN 80
            ELSE 40
        END as overall_health_score
    FROM raw.azure_customers
    WHERE updated_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    
    UNION ALL
    
    SELECT 
        'telematics_webhook_connector' as connector_name,
        'webhook' as connector_type,
        'active' as status,
        MAX(timestamp) as last_sync_time,
        5 as sync_frequency_minutes,
        COUNT(*) as records_synced_24h,
        CASE 
            WHEN DATEDIFF(minute, MAX(timestamp), CURRENT_TIMESTAMP()) <= 10 THEN 'FRESH'
            WHEN DATEDIFF(minute, MAX(timestamp), CURRENT_TIMESTAMP()) <= 30 THEN 'STALE'
            ELSE 'CRITICAL'
        END as data_freshness_status,
        0 as error_count_24h,
        CASE 
            WHEN DATEDIFF(minute, MAX(timestamp), CURRENT_TIMESTAMP()) <= 10 THEN 100
            WHEN DATEDIFF(minute, MAX(timestamp), CURRENT_TIMESTAMP()) <= 30 THEN 80
            ELSE 40
        END as overall_health_score
    FROM raw.telematics_data
    WHERE timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    
    UNION ALL
    
    SELECT 
        'traffic_api_connector' as connector_name,
        'rest_api' as connector_type,
        'active' as status,
        MAX(traffic_date) as last_sync_time,
        30 as sync_frequency_minutes,
        COUNT(*) as records_synced_24h,
        CASE 
            WHEN DATEDIFF(minute, MAX(traffic_date), CURRENT_TIMESTAMP()) <= 60 THEN 'FRESH'
            WHEN DATEDIFF(minute, MAX(traffic_date), CURRENT_TIMESTAMP()) <= 120 THEN 'STALE'
            ELSE 'CRITICAL'
        END as data_freshness_status,
        0 as error_count_24h,
        CASE 
            WHEN DATEDIFF(minute, MAX(traffic_date), CURRENT_TIMESTAMP()) <= 60 THEN 100
            WHEN DATEDIFF(minute, MAX(traffic_date), CURRENT_TIMESTAMP()) <= 120 THEN 80
            ELSE 40
        END as overall_health_score
    FROM raw.traffic_data
    WHERE traffic_date >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    
    UNION ALL
    
    SELECT 
        'weather_api_connector' as connector_name,
        'rest_api' as connector_type,
        'active' as status,
        MAX(weather_date) as last_sync_time,
        60 as sync_frequency_minutes,
        COUNT(*) as records_synced_24h,
        CASE 
            WHEN DATEDIFF(minute, MAX(weather_date), CURRENT_TIMESTAMP()) <= 120 THEN 'FRESH'
            WHEN DATEDIFF(minute, MAX(weather_date), CURRENT_TIMESTAMP()) <= 240 THEN 'STALE'
            ELSE 'CRITICAL'
        END as data_freshness_status,
        0 as error_count_24h,
        CASE 
            WHEN DATEDIFF(minute, MAX(weather_date), CURRENT_TIMESTAMP()) <= 120 THEN 100
            WHEN DATEDIFF(minute, MAX(weather_date), CURRENT_TIMESTAMP()) <= 240 THEN 80
            ELSE 40
        END as overall_health_score
    FROM raw.weather_data
    WHERE weather_date >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
);

-- 2. Connector performance metrics
CREATE OR REPLACE VIEW fivetran_connector_performance AS
SELECT 
    connector_name,
    date_trunc('day', sync_time) as sync_date,
    COUNT(*) as sync_events,
    AVG(sync_duration_minutes) as avg_sync_duration,
    MAX(sync_duration_minutes) as max_sync_duration,
    MIN(sync_duration_minutes) as min_sync_duration,
    SUM(records_synced) as total_records_synced,
    AVG(records_per_minute) as avg_records_per_minute
FROM (
    SELECT 
        'azure_sql_connector' as connector_name,
        updated_at as sync_time,
        DATEDIFF(minute, LAG(updated_at) OVER (ORDER BY updated_at), updated_at) as sync_duration_minutes,
        1 as records_synced,
        1.0 / NULLIF(DATEDIFF(minute, LAG(updated_at) OVER (ORDER BY updated_at), updated_at), 0) as records_per_minute
    FROM raw.azure_customers
    WHERE updated_at >= DATEADD(day, -7, CURRENT_DATE())
    
    UNION ALL
    
    SELECT 
        'telematics_webhook_connector' as connector_name,
        timestamp as sync_time,
        DATEDIFF(minute, LAG(timestamp) OVER (ORDER BY timestamp), timestamp) as sync_duration_minutes,
        1 as records_synced,
        1.0 / NULLIF(DATEDIFF(minute, LAG(timestamp) OVER (ORDER BY timestamp), timestamp), 0) as records_per_minute
    FROM raw.telematics_data
    WHERE timestamp >= DATEADD(day, -7, CURRENT_DATE())
    
    UNION ALL
    
    SELECT 
        'traffic_api_connector' as connector_name,
        traffic_date as sync_time,
        DATEDIFF(minute, LAG(traffic_date) OVER (ORDER BY traffic_date), traffic_date) as sync_duration_minutes,
        1 as records_synced,
        1.0 / NULLIF(DATEDIFF(minute, LAG(traffic_date) OVER (ORDER BY traffic_date), traffic_date), 0) as records_per_minute
    FROM raw.traffic_data
    WHERE traffic_date >= DATEADD(day, -7, CURRENT_DATE())
    
    UNION ALL
    
    SELECT 
        'weather_api_connector' as connector_name,
        weather_date as sync_time,
        DATEDIFF(minute, LAG(weather_date) OVER (ORDER BY weather_date), weather_date) as sync_duration_minutes,
        1 as records_synced,
        1.0 / NULLIF(DATEDIFF(minute, LAG(weather_date) OVER (ORDER BY weather_date), weather_date), 0) as records_per_minute
    FROM raw.weather_data
    WHERE weather_date >= DATEADD(day, -7, CURRENT_DATE())
)
GROUP BY connector_name, date_trunc('day', sync_time)
ORDER BY connector_name, sync_date;

-- 3. Connector alert conditions
CREATE OR REPLACE VIEW fivetran_connector_alerts AS
SELECT 
    'HEALTH_ALERT' as alert_type,
    connector_name,
    'Connector health status: ' || health_status as alert_message,
    CASE 
        WHEN health_status = 'CRITICAL' THEN 'CRITICAL'
        WHEN health_status = 'POOR' THEN 'HIGH'
        WHEN health_status = 'FAIR' THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    last_sync_time as alert_timestamp
FROM fivetran_connector_health
WHERE health_status IN ('CRITICAL', 'POOR', 'FAIR')

UNION ALL

SELECT 
    'PERFORMANCE_ALERT' as alert_type,
    connector_name,
    'Sync performance is ' || sync_performance as alert_message,
    CASE 
        WHEN sync_performance = 'POOR' THEN 'HIGH'
        WHEN sync_performance = 'FAIR' THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    sync_date as alert_timestamp
FROM fivetran_sync_performance
WHERE sync_performance IN ('POOR', 'FAIR')

UNION ALL

SELECT 
    'FREQUENCY_ALERT' as alert_type,
    connector_name,
    'Sync frequency is ' || sync_frequency_status as alert_message,
    CASE 
        WHEN sync_frequency_status = 'SLOW' THEN 'HIGH'
        WHEN sync_frequency_status = 'FAST' THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    CURRENT_TIMESTAMP() as alert_timestamp
FROM fivetran_sync_frequency
WHERE sync_frequency_status IN ('SLOW', 'FAST');

-- 4. Connector health dashboard summary
CREATE OR REPLACE VIEW fivetran_health_dashboard AS
SELECT 
    COUNT(*) as total_connectors,
    COUNT(CASE WHEN health_status = 'EXCELLENT' THEN 1 END) as excellent_connectors,
    COUNT(CASE WHEN health_status = 'GOOD' THEN 1 END) as good_connectors,
    COUNT(CASE WHEN health_status = 'FAIR' THEN 1 END) as fair_connectors,
    COUNT(CASE WHEN health_status = 'POOR' THEN 1 END) as poor_connectors,
    COUNT(CASE WHEN health_status = 'CRITICAL' THEN 1 END) as critical_connectors,
    AVG(overall_health_score) as avg_health_score,
    CASE 
        WHEN AVG(overall_health_score) >= 90 THEN 'EXCELLENT'
        WHEN AVG(overall_health_score) >= 80 THEN 'GOOD'
        WHEN AVG(overall_health_score) >= 70 THEN 'FAIR'
        WHEN AVG(overall_health_score) >= 60 THEN 'POOR'
        ELSE 'CRITICAL'
    END as overall_system_health
FROM fivetran_connector_health;
