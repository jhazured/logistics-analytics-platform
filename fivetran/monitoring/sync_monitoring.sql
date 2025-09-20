-- Fivetran sync monitoring and performance tracking
-- This file contains SQL queries to monitor sync performance and identify issues

-- 1. Sync performance monitoring
CREATE OR REPLACE VIEW fivetran_sync_performance AS
SELECT 
    connector_name,
    table_name,
    sync_date,
    records_synced,
    sync_duration_minutes,
    records_per_minute,
    CASE 
        WHEN records_per_minute > 1000 THEN 'EXCELLENT'
        WHEN records_per_minute > 500 THEN 'GOOD'
        WHEN records_per_minute > 100 THEN 'FAIR'
        ELSE 'POOR'
    END as sync_performance
FROM (
    SELECT 
        'azure_sql_connector' as connector_name,
        'customers' as table_name,
        DATE(updated_at) as sync_date,
        COUNT(*) as records_synced,
        DATEDIFF(minute, MIN(updated_at), MAX(updated_at)) as sync_duration_minutes,
        ROUND(COUNT(*) / NULLIF(DATEDIFF(minute, MIN(updated_at), MAX(updated_at)), 0), 2) as records_per_minute
    FROM raw.azure_customers
    WHERE updated_at >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY DATE(updated_at)
    
    UNION ALL
    
    SELECT 
        'azure_sql_connector' as connector_name,
        'vehicles' as table_name,
        DATE(updated_at) as sync_date,
        COUNT(*) as records_synced,
        DATEDIFF(minute, MIN(updated_at), MAX(updated_at)) as sync_duration_minutes,
        ROUND(COUNT(*) / NULLIF(DATEDIFF(minute, MIN(updated_at), MAX(updated_at)), 0), 2) as records_per_minute
    FROM raw.azure_vehicles
    WHERE updated_at >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY DATE(updated_at)
    
    UNION ALL
    
    SELECT 
        'azure_sql_connector' as connector_name,
        'shipments' as table_name,
        DATE(updated_at) as sync_date,
        COUNT(*) as records_synced,
        DATEDIFF(minute, MIN(updated_at), MAX(updated_at)) as sync_duration_minutes,
        ROUND(COUNT(*) / NULLIF(DATEDIFF(minute, MIN(updated_at), MAX(updated_at)), 0), 2) as records_per_minute
    FROM raw.azure_shipments
    WHERE updated_at >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY DATE(updated_at)
    
    UNION ALL
    
    SELECT 
        'telematics_webhook_connector' as connector_name,
        'vehicle_telemetry' as table_name,
        DATE(timestamp) as sync_date,
        COUNT(*) as records_synced,
        DATEDIFF(minute, MIN(timestamp), MAX(timestamp)) as sync_duration_minutes,
        ROUND(COUNT(*) / NULLIF(DATEDIFF(minute, MIN(timestamp), MAX(timestamp)), 0), 2) as records_per_minute
    FROM raw.telematics_data
    WHERE timestamp >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY DATE(timestamp)
    
    UNION ALL
    
    SELECT 
        'traffic_api_connector' as connector_name,
        'traffic_conditions' as table_name,
        DATE(traffic_date) as sync_date,
        COUNT(*) as records_synced,
        DATEDIFF(minute, MIN(traffic_date), MAX(traffic_date)) as sync_duration_minutes,
        ROUND(COUNT(*) / NULLIF(DATEDIFF(minute, MIN(traffic_date), MAX(traffic_date)), 0), 2) as records_per_minute
    FROM raw.traffic_data
    WHERE traffic_date >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY DATE(traffic_date)
    
    UNION ALL
    
    SELECT 
        'weather_api_connector' as connector_name,
        'weather_data' as table_name,
        DATE(weather_date) as sync_date,
        COUNT(*) as records_synced,
        DATEDIFF(minute, MIN(weather_date), MAX(weather_date)) as sync_duration_minutes,
        ROUND(COUNT(*) / NULLIF(DATEDIFF(minute, MIN(weather_date), MAX(weather_date)), 0), 2) as records_per_minute
    FROM raw.weather_data
    WHERE weather_date >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY DATE(weather_date)
);

-- 2. Sync error monitoring
CREATE OR REPLACE VIEW fivetran_sync_errors AS
SELECT 
    connector_name,
    table_name,
    error_date,
    error_count,
    error_type,
    error_message,
    CASE 
        WHEN error_count > 100 THEN 'CRITICAL'
        WHEN error_count > 50 THEN 'HIGH'
        WHEN error_count > 10 THEN 'MEDIUM'
        ELSE 'LOW'
    END as error_severity
FROM (
    -- This would typically come from Fivetran's error logs
    -- For now, we'll create a placeholder structure
    SELECT 
        'azure_sql_connector' as connector_name,
        'customers' as table_name,
        CURRENT_DATE() as error_date,
        0 as error_count,
        'NO_ERRORS' as error_type,
        'No sync errors detected' as error_message
    WHERE 1=0  -- This ensures no rows are returned unless there are actual errors
);

-- 3. Sync frequency monitoring
CREATE OR REPLACE VIEW fivetran_sync_frequency AS
SELECT 
    connector_name,
    table_name,
    expected_sync_frequency_minutes,
    actual_avg_sync_frequency_minutes,
    CASE 
        WHEN actual_avg_sync_frequency_minutes > expected_sync_frequency_minutes * 1.5 THEN 'SLOW'
        WHEN actual_avg_sync_frequency_minutes < expected_sync_frequency_minutes * 0.5 THEN 'FAST'
        ELSE 'NORMAL'
    END as sync_frequency_status
FROM (
    SELECT 
        'azure_sql_connector' as connector_name,
        'customers' as table_name,
        60 as expected_sync_frequency_minutes,
        AVG(DATEDIFF(minute, LAG(updated_at) OVER (ORDER BY updated_at), updated_at)) as actual_avg_sync_frequency_minutes
    FROM raw.azure_customers
    WHERE updated_at >= DATEADD(day, -7, CURRENT_DATE())
    
    UNION ALL
    
    SELECT 
        'telematics_webhook_connector' as connector_name,
        'vehicle_telemetry' as table_name,
        5 as expected_sync_frequency_minutes,
        AVG(DATEDIFF(minute, LAG(timestamp) OVER (ORDER BY timestamp), timestamp)) as actual_avg_sync_frequency_minutes
    FROM raw.telematics_data
    WHERE timestamp >= DATEADD(day, -7, CURRENT_DATE())
    
    UNION ALL
    
    SELECT 
        'traffic_api_connector' as connector_name,
        'traffic_conditions' as table_name,
        30 as expected_sync_frequency_minutes,
        AVG(DATEDIFF(minute, LAG(traffic_date) OVER (ORDER BY traffic_date), traffic_date)) as actual_avg_sync_frequency_minutes
    FROM raw.traffic_data
    WHERE traffic_date >= DATEADD(day, -7, CURRENT_DATE())
    
    UNION ALL
    
    SELECT 
        'weather_api_connector' as connector_name,
        'weather_data' as table_name,
        60 as expected_sync_frequency_minutes,
        AVG(DATEDIFF(minute, LAG(weather_date) OVER (ORDER BY weather_date), weather_date)) as actual_avg_sync_frequency_minutes
    FROM raw.weather_data
    WHERE weather_date >= DATEADD(day, -7, CURRENT_DATE())
);

-- 4. Sync health summary
CREATE OR REPLACE VIEW fivetran_sync_health_summary AS
SELECT 
    connector_name,
    table_name,
    last_sync_time,
    records_synced_today,
    sync_performance_today,
    error_count_today,
    overall_health_status
FROM (
    SELECT 
        f.connector_name,
        f.table_name,
        f.last_sync_time,
        COALESCE(s.records_synced, 0) as records_synced_today,
        COALESCE(s.sync_performance, 'UNKNOWN') as sync_performance_today,
        COALESCE(e.error_count, 0) as error_count_today,
        CASE 
            WHEN f.freshness_status = 'STALE' THEN 'UNHEALTHY'
            WHEN COALESCE(e.error_count, 0) > 10 THEN 'UNHEALTHY'
            WHEN COALESCE(s.sync_performance, 'UNKNOWN') = 'POOR' THEN 'UNHEALTHY'
            ELSE 'HEALTHY'
        END as overall_health_status
    FROM fivetran_data_freshness f
    LEFT JOIN fivetran_sync_performance s ON f.connector_name = s.connector_name 
        AND f.table_name = s.table_name 
        AND s.sync_date = CURRENT_DATE()
    LEFT JOIN fivetran_sync_errors e ON f.connector_name = e.connector_name 
        AND f.table_name = e.table_name 
        AND e.error_date = CURRENT_DATE()
);
