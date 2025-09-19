-- Warehouse Cost Analysis View
CREATE OR REPLACE VIEW ANALYTICS.view_warehouse_cost_analysis AS
SELECT 
    warehouse_name,
    DATE_TRUNC('day', start_time) AS usage_date,
    SUM(credits_used) AS daily_credits,
    ROUND(SUM(credits_used) * 2.0, 2) AS estimated_daily_cost_usd, -- Assume $2/credit
    COUNT(DISTINCT query_id) AS query_count,
    AVG(execution_time) / 1000 AS avg_execution_seconds,
    SUM(CASE WHEN execution_time > 300000 THEN 1 ELSE 0 END) AS long_running_queries,
    MAX(credits_used) AS max_single_query_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= CURRENT_DATE() - 30
GROUP BY 1, 2
ORDER BY usage_date DESC, daily_credits DESC;