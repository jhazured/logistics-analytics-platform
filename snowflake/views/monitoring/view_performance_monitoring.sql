-- Snowflake Performance Monitoring Dashboard
-- Comprehensive performance metrics for the logistics analytics platform

CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.MONITORING.V_PERFORMANCE_MONITORING AS
WITH query_performance AS (
    SELECT 
        QUERY_ID,
        QUERY_TEXT,
        USER_NAME,
        ROLE_NAME,
        WAREHOUSE_NAME,
        DATABASE_NAME,
        SCHEMA_NAME,
        START_TIME,
        END_TIME,
        TOTAL_ELAPSED_TIME,
        BYTES_SCANNED,
        BYTES_WRITTEN,
        ROWS_PRODUCED,
        ROWS_INSERTED,
        ROWS_UPDATED,
        ROWS_DELETED,
        CREDITS_USED_CLOUD_SERVICES,
        CREDITS_USED_COMPUTE,
        CASE 
            WHEN TOTAL_ELAPSED_TIME > 300000 THEN 'SLOW'  -- 5 minutes
            WHEN TOTAL_ELAPSED_TIME > 60000 THEN 'MEDIUM'  -- 1 minute
            ELSE 'FAST'
        END as performance_category
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= CURRENT_DATE() - 7
),

warehouse_usage AS (
    SELECT 
        WAREHOUSE_NAME,
        DATE(START_TIME) as usage_date,
        SUM(CREDITS_USED) as total_credits,
        AVG(CREDITS_USED) as avg_credits,
        COUNT(*) as query_count,
        AVG(AVG_RUNNING) as avg_running_queries,
        AVG(AVG_QUEUED_LOAD) as avg_queued_queries
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= CURRENT_DATE() - 30
    GROUP BY 1, 2
),

table_usage AS (
    SELECT 
        TABLE_NAME,
        TABLE_SCHEMA,
        TABLE_CATALOG,
        ROW_COUNT,
        BYTES,
        LAST_ALTERED,
        CLUSTERING_KEY,
        CASE 
            WHEN BYTES > 1000000000 THEN 'LARGE'  -- 1GB
            WHEN BYTES > 100000000 THEN 'MEDIUM'   -- 100MB
            ELSE 'SMALL'
        END as size_category
    FROM SNOWFLAKE.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_CATALOG = 'LOGISTICS_DW_PROD'
),

dbt_run_performance AS (
    SELECT 
        'dbt_run' as metric_type,
        CURRENT_TIMESTAMP() as measurement_time,
        COUNT(*) as total_models,
        SUM(CASE WHEN STATUS = 'success' THEN 1 ELSE 0 END) as successful_models,
        SUM(CASE WHEN STATUS = 'error' THEN 1 ELSE 0 END) as failed_models,
        AVG(EXECUTION_TIME) as avg_execution_time,
        MAX(EXECUTION_TIME) as max_execution_time
    FROM LOGISTICS_DW_PROD.MONITORING.DBT_RUN_RESULTS
    WHERE RUN_STARTED_AT >= CURRENT_DATE() - 1
)

SELECT 
    'query_performance' as metric_category,
    performance_category,
    COUNT(*) as count,
    AVG(TOTAL_ELAPSED_TIME) as avg_elapsed_time,
    AVG(BYTES_SCANNED) as avg_bytes_scanned,
    AVG(CREDITS_USED_COMPUTE) as avg_credits_used
FROM query_performance
GROUP BY 1, 2

UNION ALL

SELECT 
    'warehouse_usage' as metric_category,
    WAREHOUSE_NAME as performance_category,
    COUNT(*) as count,
    AVG(total_credits) as avg_elapsed_time,
    AVG(avg_credits) as avg_bytes_scanned,
    AVG(query_count) as avg_credits_used
FROM warehouse_usage
GROUP BY 1, 2

UNION ALL

SELECT 
    'table_usage' as metric_category,
    size_category as performance_category,
    COUNT(*) as count,
    AVG(BYTES) as avg_elapsed_time,
    AVG(ROW_COUNT) as avg_bytes_scanned,
    NULL as avg_credits_used
FROM table_usage
GROUP BY 1, 2

UNION ALL

SELECT 
    'dbt_performance' as metric_category,
    'overall' as performance_category,
    total_models as count,
    avg_execution_time as avg_elapsed_time,
    successful_models as avg_bytes_scanned,
    failed_models as avg_credits_used
FROM dbt_run_performance
