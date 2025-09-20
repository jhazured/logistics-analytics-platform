-- Create monitoring tasks for real-time alerts

-- Task 1: Vehicle monitoring alerts
CREATE OR REPLACE TASK TSK_vehicle_monitoring_alerts
WAREHOUSE = COMPUTE_WH_XS
SCHEDULE = '30 SECONDS'
COMMENT = 'Monitor vehicle telemetry for real-time alerts'
AS
INSERT INTO LOGISTICS_DW_PROD.MONITORING.TBL_real_time_vehicle_alerts (vehicle_id, alert_type, severity, message)
WITH telemetry_updates AS (
    SELECT 
        vehicle_id,
        engine_temp_c,
        fuel_level_percent,
        speed_kmh,
        latitude,
        longitude,
        timestamp
    FROM STR_vehicle_telemetry_stream
    WHERE METADATA$ACTION = 'INSERT'
),
alerts AS (
    SELECT 
        vehicle_id,
        CASE 
            WHEN engine_temp_c > 95 THEN 'HIGH_ENGINE_TEMP'
            WHEN fuel_level_percent < 10 THEN 'LOW_FUEL'
            WHEN speed_kmh > 120 THEN 'SPEEDING'
            ELSE NULL
        END as alert_type,
        CASE 
            WHEN engine_temp_c > 95 THEN 'CRITICAL'
            WHEN fuel_level_percent < 10 THEN 'HIGH'
            WHEN speed_kmh > 120 THEN 'MEDIUM'
            ELSE NULL
        END as severity,
        CASE 
            WHEN engine_temp_c > 95 THEN 'Engine temperature critical: ' || engine_temp_c || '°C'
            WHEN fuel_level_percent < 10 THEN 'Low fuel warning: ' || fuel_level_percent || '%'
            WHEN speed_kmh > 120 THEN 'Speeding alert: ' || speed_kmh || ' km/h'
            ELSE NULL
        END as message
    FROM telemetry_updates
)
SELECT vehicle_id, alert_type, severity, message
FROM alerts
WHERE alert_type IS NOT NULL;

-- Task 2: System performance monitoring
CREATE OR REPLACE TASK TSK_system_performance_monitoring
WAREHOUSE = COMPUTE_WH_XS
SCHEDULE = '1 MINUTE'
COMMENT = 'Monitor system performance and health'
AS
INSERT INTO LOGISTICS_DW_PROD.MONITORING.TBL_system_alerts (alert_type, severity, component, message)
SELECT 
    'HIGH_QUERY_COST' as alert_type,
    'HIGH' as severity,
    'WAREHOUSE' as component,
    'High query cost detected: ' || credits_used || ' credits for query ' || query_id
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL '5 MINUTES'
AND credits_used > 10
AND query_id NOT IN (
    SELECT DISTINCT SUBSTRING(message, POSITION('query ' IN message) + 6, 36)
    FROM LOGISTICS_DW_PROD.MONITORING.TBL_system_alerts 
    WHERE alert_type = 'HIGH_QUERY_COST' 
    AND alert_timestamp >= CURRENT_TIMESTAMP() - INTERVAL '1 HOUR'
);

-- Enable tasks
ALTER TASK TSK_vehicle_monitoring_alerts RESUME;
ALTER TASK TSK_system_performance_monitoring RESUME;
