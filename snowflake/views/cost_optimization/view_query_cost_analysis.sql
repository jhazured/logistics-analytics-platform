-- Query Performance and Cost View
CREATE OR REPLACE VIEW ANALYTICS.view_query_cost_analysis AS
SELECT 
    DATE_TRUNC('hour', start_time) AS query_hour,
    warehouse_name,
    user_name,
    query_type,
    database_name,
    schema_name,
    COUNT(*) AS query_count,
    SUM(credits_used) AS total_credits,
    ROUND(SUM(credits_used) * 2.0, 2) AS estimated_cost_usd,
    AVG(execution_time) / 1000 AS avg_execution_seconds,
    SUM(bytes_scanned) / POWER(1024, 3) AS total_gb_scanned,
    SUM(CASE WHEN error_code IS NOT NULL THEN 1 ELSE 0 END) AS failed_queries
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= CURRENT_DATE() - 7
    AND credits_used > 0
GROUP BY 1,2,3,4,5,6
ORDER BY total_credits DESC;