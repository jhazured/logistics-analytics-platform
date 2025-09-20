-- Deployment script for streams and tasks
-- Run this after dbt models are deployed to create streams and tasks

-- Step 1: Create streams (run after dbt models are materialized)
-- Note: Replace {{ ref('table_name') }} with actual table names after dbt deployment

-- Create streams on fact tables
CREATE OR REPLACE STREAM shipments_stream ON TABLE LOGISTICS_ANALYTICS.MARTS.FACT_SHIPMENTS
COMMENT = 'Stream for real-time shipment updates';

CREATE OR REPLACE STREAM vehicle_telemetry_stream ON TABLE LOGISTICS_ANALYTICS.MARTS.FACT_VEHICLE_TELEMETRY
COMMENT = 'Stream for real-time vehicle telemetry updates';

CREATE OR REPLACE STREAM route_performance_stream ON TABLE LOGISTICS_ANALYTICS.MARTS.FACT_ROUTE_PERFORMANCE
COMMENT = 'Stream for real-time route performance updates';

-- Create streams on staging tables for early change detection
CREATE OR REPLACE STREAM stg_shipments_stream ON TABLE LOGISTICS_ANALYTICS.STAGING.STG_SHIPMENTS
COMMENT = 'Stream for staging shipment data changes';

CREATE OR REPLACE STREAM stg_vehicle_telemetry_stream ON TABLE LOGISTICS_ANALYTICS.STAGING.STG_VEHICLE_TELEMETRY
COMMENT = 'Stream for staging vehicle telemetry data changes';

-- Step 2: Create real-time KPI table
CREATE OR REPLACE TABLE real_time_kpis (
    kpi_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    metric_name VARCHAR(100),
    metric_value FLOAT,
    dimensions VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Real-time KPI storage for streaming analytics';

-- Step 3: Create vehicle alerts table
CREATE OR REPLACE TABLE real_time_vehicle_alerts (
    alert_id VARCHAR(50) DEFAULT UUID_STRING(),
    vehicle_id VARCHAR(20),
    alert_type VARCHAR(50),
    severity VARCHAR(20),
    message TEXT,
    alert_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    resolved_at TIMESTAMP_NTZ,
    resolved_by VARCHAR(100)
);

-- Step 4: Create and enable tasks
-- Task 1: Process shipment updates
CREATE OR REPLACE TASK process_shipment_updates
WAREHOUSE = COMPUTE_WH_SMALL
SCHEDULE = '1 MINUTE'
COMMENT = 'Process real-time shipment updates'
AS
INSERT INTO real_time_kpis (metric_name, metric_value, dimensions)
WITH shipment_updates AS (
    SELECT 
        shipment_id,
        customer_id,
        planned_duration_minutes,
        actual_duration_minutes,
        is_on_time,
        revenue,
        METADATA$ACTION as action_type,
        METADATA$ISUPDATE as is_update
    FROM shipments_stream
    WHERE METADATA$ACTION IN ('INSERT', 'UPDATE')
),
real_time_metrics AS (
    -- On-time delivery rate
    SELECT 
        'on_time_delivery_rate' as metric_name,
        AVG(is_on_time::FLOAT) * 100 as metric_value,
        OBJECT_CONSTRUCT('timeframe', 'last_hour') as dimensions
    FROM shipment_updates
    
    UNION ALL
    
    -- Average delivery time (convert minutes to hours)
    SELECT 
        'avg_delivery_time_hours' as metric_name,
        AVG(actual_duration_minutes) / 60.0 as metric_value,
        OBJECT_CONSTRUCT('timeframe', 'last_hour') as dimensions
    FROM shipment_updates
    
    UNION ALL
    
    -- Revenue per hour
    SELECT 
        'revenue_per_hour' as metric_name,
        SUM(revenue) as metric_value,
        OBJECT_CONSTRUCT('timeframe', 'current_hour') as dimensions
    FROM shipment_updates
)
SELECT * FROM real_time_metrics;

-- Task 2: Vehicle monitoring alerts
CREATE OR REPLACE TASK vehicle_monitoring_alerts
WAREHOUSE = COMPUTE_WH_XS
SCHEDULE = '30 SECONDS'
COMMENT = 'Monitor vehicle telemetry for real-time alerts'
AS
INSERT INTO real_time_vehicle_alerts (vehicle_id, alert_type, severity, message)
WITH telemetry_updates AS (
    SELECT 
        vehicle_id,
        engine_temp_c,
        fuel_level_percent,
        speed_kmh,
        latitude,
        longitude,
        timestamp
    FROM vehicle_telemetry_stream
    WHERE METADATA$ACTION = 'INSERT'
),
alerts AS (
    -- Engine temperature alerts
    SELECT 
        vehicle_id,
        'ENGINE_OVERHEATING' as alert_type,
        CASE WHEN engine_temp_c > 100 THEN 'CRITICAL' ELSE 'WARNING' END as severity,
        'Engine temperature: ' || engine_temp_c || '°C' as message
    FROM telemetry_updates
    WHERE engine_temp_c > 90
    
    UNION ALL
    
    -- Low fuel alerts
    SELECT 
        vehicle_id,
        'LOW_FUEL' as alert_type,
        CASE WHEN fuel_level_percent < 10 THEN 'CRITICAL' ELSE 'WARNING' END as severity,
        'Fuel level: ' || fuel_level_percent || '%' as message
    FROM telemetry_updates
    WHERE fuel_level_percent < 20
    
    UNION ALL
    
    -- Speeding alerts
    SELECT 
        vehicle_id,
        'SPEEDING' as alert_type,
        'WARNING' as severity,
        'Speed: ' || speed_kmh || ' km/h' as message
    FROM telemetry_updates
    WHERE speed_kmh > 120
)
SELECT * FROM alerts;

-- Task 3: Task health monitoring
CREATE OR REPLACE TASK task_health_monitor
WAREHOUSE = COMPUTE_WH_XS
SCHEDULE = '5 MINUTES'
COMMENT = 'Monitor task health and send alerts for failures'
AS
INSERT INTO real_time_vehicle_alerts (vehicle_id, alert_type, severity, message)
SELECT 
    'SYSTEM' as vehicle_id,
    'TASK_FAILURE' as alert_type,
    'CRITICAL' as severity,
    'Task ' || task_name || ' has failed: ' || COALESCE(error_message, 'Unknown error') as message
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE task_database = CURRENT_DATABASE()
  AND state = 'FAILED'
  AND scheduled_time > DATEADD('minute', -10, CURRENT_TIMESTAMP());

-- Step 5: Enable all tasks
ALTER TASK process_shipment_updates RESUME;
ALTER TASK vehicle_monitoring_alerts RESUME;
ALTER TASK task_health_monitor RESUME;

-- Step 6: Create monitoring views
CREATE OR REPLACE VIEW task_execution_monitor AS
SELECT 
    task_name,
    task_schema,
    task_database,
    state,
    schedule,
    warehouse_name,
    last_successful_run,
    last_failed_run,
    next_scheduled_time,
    error_message,
    CASE 
        WHEN state = 'SUSPENDED' THEN 'CRITICAL'
        WHEN last_failed_run > last_successful_run THEN 'WARNING'
        WHEN last_successful_run < DATEADD('hour', -2, CURRENT_TIMESTAMP()) THEN 'WARNING'
        ELSE 'HEALTHY'
    END as health_status
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE task_database = CURRENT_DATABASE()
QUALIFY ROW_NUMBER() OVER (PARTITION BY task_name ORDER BY scheduled_time DESC) = 1;

-- Step 7: Grant permissions
GRANT USAGE ON WAREHOUSE COMPUTE_WH_SMALL TO ROLE ANALYST_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH_XS TO ROLE ANALYST_ROLE;
GRANT SELECT ON VIEW task_execution_monitor TO ROLE ANALYST_ROLE;
GRANT SELECT ON TABLE real_time_kpis TO ROLE ANALYST_ROLE;
GRANT SELECT ON TABLE real_time_vehicle_alerts TO ROLE ANALYST_ROLE;

-- Step 8: Create cleanup procedure
CREATE OR REPLACE PROCEDURE cleanup_old_stream_data()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Clean up old KPI data (keep last 7 days)
    DELETE FROM real_time_kpis 
    WHERE created_at < DATEADD('day', -7, CURRENT_TIMESTAMP());
    
    -- Clean up resolved alerts (keep last 30 days)
    DELETE FROM real_time_vehicle_alerts 
    WHERE resolved_at IS NOT NULL 
      AND resolved_at < DATEADD('day', -30, CURRENT_TIMESTAMP());
    
    RETURN 'Cleanup completed successfully';
END;
$$;

-- Create cleanup task
CREATE OR REPLACE TASK cleanup_stream_data_task
WAREHOUSE = COMPUTE_WH_XS
SCHEDULE = 'USING CRON 0 2 * * *'  -- 2 AM daily
AS
    CALL cleanup_old_stream_data();

ALTER TASK cleanup_stream_data_task RESUME;
