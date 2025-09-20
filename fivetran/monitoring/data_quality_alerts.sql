-- Fivetran data quality monitoring and alerting
-- This file contains SQL queries to monitor data quality from Fivetran connectors

-- 1. Data freshness monitoring
CREATE OR REPLACE VIEW fivetran_data_freshness AS
SELECT 
    connector_name,
    table_name,
    last_sync_time,
    DATEDIFF(minute, last_sync_time, CURRENT_TIMESTAMP()) as minutes_since_last_sync,
    CASE 
        WHEN DATEDIFF(minute, last_sync_time, CURRENT_TIMESTAMP()) > 120 THEN 'STALE'
        WHEN DATEDIFF(minute, last_sync_time, CURRENT_TIMESTAMP()) > 60 THEN 'WARNING'
        ELSE 'FRESH'
    END as freshness_status
FROM (
    SELECT 
        'azure_sql_connector' as connector_name,
        'customers' as table_name,
        MAX(updated_at) as last_sync_time
    FROM raw.azure_customers
    
    UNION ALL
    
    SELECT 
        'azure_sql_connector' as connector_name,
        'vehicles' as table_name,
        MAX(updated_at) as last_sync_time
    FROM raw.azure_vehicles
    
    UNION ALL
    
    SELECT 
        'azure_sql_connector' as connector_name,
        'shipments' as table_name,
        MAX(updated_at) as last_sync_time
    FROM raw.azure_shipments
    
    UNION ALL
    
    SELECT 
        'telematics_webhook_connector' as connector_name,
        'vehicle_telemetry' as table_name,
        MAX(timestamp) as last_sync_time
    FROM raw.telematics_data
    
    UNION ALL
    
    SELECT 
        'traffic_api_connector' as connector_name,
        'traffic_conditions' as table_name,
        MAX(traffic_date) as last_sync_time
    FROM raw.traffic_data
    
    UNION ALL
    
    SELECT 
        'weather_api_connector' as connector_name,
        'weather_data' as table_name,
        MAX(weather_date) as last_sync_time
    FROM raw.weather_data
);

-- 2. Data volume monitoring
CREATE OR REPLACE VIEW fivetran_data_volume_monitoring AS
SELECT 
    connector_name,
    table_name,
    record_count,
    data_size_mb,
    CASE 
        WHEN record_count = 0 THEN 'NO_DATA'
        WHEN record_count < 100 THEN 'LOW_VOLUME'
        WHEN record_count < 1000 THEN 'MEDIUM_VOLUME'
        ELSE 'HIGH_VOLUME'
    END as volume_status
FROM (
    SELECT 
        'azure_sql_connector' as connector_name,
        'customers' as table_name,
        COUNT(*) as record_count,
        ROUND(SUM(LENGTH(TO_JSON(OBJECT_CONSTRUCT(*)))) / 1024 / 1024, 2) as data_size_mb
    FROM raw.azure_customers
    
    UNION ALL
    
    SELECT 
        'azure_sql_connector' as connector_name,
        'vehicles' as table_name,
        COUNT(*) as record_count,
        ROUND(SUM(LENGTH(TO_JSON(OBJECT_CONSTRUCT(*)))) / 1024 / 1024, 2) as data_size_mb
    FROM raw.azure_vehicles
    
    UNION ALL
    
    SELECT 
        'azure_sql_connector' as connector_name,
        'shipments' as table_name,
        COUNT(*) as record_count,
        ROUND(SUM(LENGTH(TO_JSON(OBJECT_CONSTRUCT(*)))) / 1024 / 1024, 2) as data_size_mb
    FROM raw.azure_shipments
    
    UNION ALL
    
    SELECT 
        'telematics_webhook_connector' as connector_name,
        'vehicle_telemetry' as table_name,
        COUNT(*) as record_count,
        ROUND(SUM(LENGTH(TO_JSON(OBJECT_CONSTRUCT(*)))) / 1024 / 1024, 2) as data_size_mb
    FROM raw.telematics_data
    
    UNION ALL
    
    SELECT 
        'traffic_api_connector' as connector_name,
        'traffic_conditions' as table_name,
        COUNT(*) as record_count,
        ROUND(SUM(LENGTH(TO_JSON(OBJECT_CONSTRUCT(*)))) / 1024 / 1024, 2) as data_size_mb
    FROM raw.traffic_data
    
    UNION ALL
    
    SELECT 
        'weather_api_connector' as connector_name,
        'weather_data' as table_name,
        COUNT(*) as record_count,
        ROUND(SUM(LENGTH(TO_JSON(OBJECT_CONSTRUCT(*)))) / 1024 / 1024, 2) as data_size_mb
    FROM raw.weather_data
);

-- 3. Data quality validation
CREATE OR REPLACE VIEW fivetran_data_quality_validation AS
SELECT 
    connector_name,
    table_name,
    validation_rule,
    validation_result,
    CASE 
        WHEN validation_result = 'PASS' THEN 'GOOD'
        WHEN validation_result = 'FAIL' THEN 'BAD'
        ELSE 'UNKNOWN'
    END as quality_status
FROM (
    -- Check for null values in critical fields
    SELECT 
        'azure_sql_connector' as connector_name,
        'customers' as table_name,
        'no_null_customer_ids' as validation_rule,
        CASE WHEN COUNT(*) = COUNT(customer_id) THEN 'PASS' ELSE 'FAIL' END as validation_result
    FROM raw.azure_customers
    
    UNION ALL
    
    SELECT 
        'azure_sql_connector' as connector_name,
        'vehicles' as table_name,
        'no_null_vehicle_ids' as validation_rule,
        CASE WHEN COUNT(*) = COUNT(vehicle_id) THEN 'PASS' ELSE 'FAIL' END as validation_result
    FROM raw.azure_vehicles
    
    UNION ALL
    
    SELECT 
        'azure_sql_connector' as connector_name,
        'shipments' as table_name,
        'no_null_shipment_ids' as validation_rule,
        CASE WHEN COUNT(*) = COUNT(shipment_id) THEN 'PASS' ELSE 'FAIL' END as validation_result
    FROM raw.azure_shipments
    
    UNION ALL
    
    SELECT 
        'telematics_webhook_connector' as connector_name,
        'vehicle_telemetry' as table_name,
        'no_null_vehicle_ids' as validation_rule,
        CASE WHEN COUNT(*) = COUNT(vehicle_id) THEN 'PASS' ELSE 'FAIL' END as validation_result
    FROM raw.telematics_data
    
    UNION ALL
    
    SELECT 
        'traffic_api_connector' as connector_name,
        'traffic_conditions' as table_name,
        'no_null_route_ids' as validation_rule,
        CASE WHEN COUNT(*) = COUNT(route_id) THEN 'PASS' ELSE 'FAIL' END as validation_result
    FROM raw.traffic_data
    
    UNION ALL
    
    SELECT 
        'weather_api_connector' as connector_name,
        'weather_data' as table_name,
        'no_null_location_ids' as validation_rule,
        CASE WHEN COUNT(*) = COUNT(location_id) THEN 'PASS' ELSE 'FAIL' END as validation_result
    FROM raw.weather_data
);

-- 4. Alert conditions
CREATE OR REPLACE VIEW fivetran_alert_conditions AS
SELECT 
    'DATA_FRESHNESS_ALERT' as alert_type,
    connector_name,
    table_name,
    'Data is stale - last sync was ' || minutes_since_last_sync || ' minutes ago' as alert_message,
    'HIGH' as severity
FROM fivetran_data_freshness
WHERE freshness_status = 'STALE'

UNION ALL

SELECT 
    'DATA_VOLUME_ALERT' as alert_type,
    connector_name,
    table_name,
    'Data volume is ' || volume_status || ' - ' || record_count || ' records' as alert_message,
    CASE 
        WHEN volume_status = 'NO_DATA' THEN 'CRITICAL'
        WHEN volume_status = 'LOW_VOLUME' THEN 'MEDIUM'
        ELSE 'INFO'
    END as severity
FROM fivetran_data_volume_monitoring
WHERE volume_status IN ('NO_DATA', 'LOW_VOLUME')

UNION ALL

SELECT 
    'DATA_QUALITY_ALERT' as alert_type,
    connector_name,
    table_name,
    'Data quality validation failed: ' || validation_rule as alert_message,
    'HIGH' as severity
FROM fivetran_data_quality_validation
WHERE quality_status = 'BAD';
