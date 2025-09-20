-- Enhanced Task Management System for Logistics Analytics Platform
-- This script provides comprehensive task monitoring and management capabilities

-- Task monitoring view
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
  AND task_schema = CURRENT_SCHEMA()
QUALIFY ROW_NUMBER() OVER (PARTITION BY task_name ORDER BY scheduled_time DESC) = 1;

-- Task performance metrics
CREATE OR REPLACE VIEW task_performance_metrics AS
SELECT 
    task_name,
    DATE_TRUNC('day', scheduled_time) as execution_date,
    COUNT(*) as total_executions,
    SUM(CASE WHEN state = 'SUCCEEDED' THEN 1 ELSE 0 END) as successful_executions,
    SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) as failed_executions,
    ROUND(AVG(EXECUTION_TIME_MS), 2) as avg_execution_time_ms,
    ROUND(MAX(EXECUTION_TIME_MS), 2) as max_execution_time_ms,
    ROUND(100.0 * SUM(CASE WHEN state = 'SUCCEEDED' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE task_database = CURRENT_DATABASE()
  AND scheduled_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY task_name, DATE_TRUNC('day', scheduled_time)
ORDER BY execution_date DESC, task_name;

-- Task alert system
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
FROM task_execution_monitor
WHERE health_status = 'CRITICAL'
  AND last_failed_run > DATEADD('minute', -10, CURRENT_TIMESTAMP());

-- Enable task monitoring
ALTER TASK task_health_monitor RESUME;

-- Task management procedures
CREATE OR REPLACE PROCEDURE SP_suspend_all_tasks()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    task_cursor CURSOR FOR 
        SELECT task_name, task_schema, task_database 
        FROM SNOWFLAKE.ACCOUNT_USAGE.TASKS 
        WHERE task_database = CURRENT_DATABASE();
    task_record RECORD;
    result STRING := '';
BEGIN
    FOR task_record IN task_cursor DO
        EXECUTE IMMEDIATE 'ALTER TASK ' || task_record.task_database || '.' || 
                         task_record.task_schema || '.' || task_record.task_name || ' SUSPEND';
        result := result || 'Suspended: ' || task_record.task_name || '; ';
    END FOR;
    RETURN result;
END;
$$;

CREATE OR REPLACE PROCEDURE SP_resume_all_tasks()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    task_cursor CURSOR FOR 
        SELECT task_name, task_schema, task_database 
        FROM SNOWFLAKE.ACCOUNT_USAGE.TASKS 
        WHERE task_database = CURRENT_DATABASE();
    task_record RECORD;
    result STRING := '';
BEGIN
    FOR task_record IN task_cursor DO
        EXECUTE IMMEDIATE 'ALTER TASK ' || task_record.task_database || '.' || 
                         task_record.task_schema || '.' || task_record.task_name || ' RESUME';
        result := result || 'Resumed: ' || task_record.task_name || '; ';
    END FOR;
    RETURN result;
END;
$$;

-- Stream monitoring
CREATE OR REPLACE VIEW stream_monitor AS
SELECT 
    stream_name,
    stream_schema,
    stream_database,
    source_table_name,
    source_schema_name,
    source_database_name,
    stale_after,
    stale_after_seconds,
    CASE 
        WHEN stale_after_seconds > 3600 THEN 'STALE'
        WHEN stale_after_seconds > 1800 THEN 'WARNING'
        ELSE 'FRESH'
    END as freshness_status
FROM SNOWFLAKE.ACCOUNT_USAGE.STREAMS
WHERE stream_database = CURRENT_DATABASE()
ORDER BY stale_after_seconds DESC;

-- Stream processing efficiency metrics
CREATE OR REPLACE VIEW stream_processing_metrics AS
SELECT 
    stream_name,
    DATE_TRUNC('hour', stream_timestamp) as processing_hour,
    COUNT(*) as records_processed,
    AVG(EXECUTION_TIME_MS) as avg_processing_time_ms,
    MAX(EXECUTION_TIME_MS) as max_processing_time_ms
FROM SNOWFLAKE.ACCOUNT_USAGE.STREAM_HISTORY
WHERE stream_database = CURRENT_DATABASE()
  AND stream_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY stream_name, DATE_TRUNC('hour', stream_timestamp)
ORDER BY processing_hour DESC, stream_name;
