-- Snowflake Cost Monitoring and Alerting
-- Comprehensive cost tracking with email-based alerting

CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.MONITORING.V_COST_MONITORING AS
WITH daily_costs AS (
    SELECT 
        DATE(USAGE_DATE) as cost_date,
        WAREHOUSE_NAME,
        SUM(CREDITS_USED) as daily_credits,
        SUM(CREDITS_USED) * 3.0 as estimated_cost_usd,  -- $3 per credit estimate
        COUNT(*) as query_count,
        AVG(CREDITS_USED) as avg_credits_per_query
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE USAGE_DATE >= CURRENT_DATE() - 30
    GROUP BY 1, 2
),

monthly_costs AS (
    SELECT 
        DATE_TRUNC('month', cost_date) as month,
        WAREHOUSE_NAME,
        SUM(daily_credits) as monthly_credits,
        SUM(estimated_cost_usd) as monthly_cost_usd,
        AVG(daily_credits) as avg_daily_credits,
        MAX(daily_credits) as max_daily_credits
    FROM daily_costs
    GROUP BY 1, 2
),

cost_trends AS (
    SELECT 
        WAREHOUSE_NAME,
        monthly_cost_usd,
        LAG(monthly_cost_usd, 1) OVER (PARTITION BY WAREHOUSE_NAME ORDER BY month) as previous_month_cost,
        CASE 
            WHEN LAG(monthly_cost_usd, 1) OVER (PARTITION BY WAREHOUSE_NAME ORDER BY month) > 0
            THEN (monthly_cost_usd / LAG(monthly_cost_usd, 1) OVER (PARTITION BY WAREHOUSE_NAME ORDER BY month) - 1) * 100
            ELSE NULL
        END as cost_change_percent
    FROM monthly_costs
    WHERE month = DATE_TRUNC('month', CURRENT_DATE())
),

cost_alerts AS (
    SELECT 
        'daily_budget_exceeded' as alert_type,
        cost_date,
        WAREHOUSE_NAME,
        estimated_cost_usd,
        100.0 as budget_threshold,  -- $100 daily budget
        'HIGH' as severity,
        'Daily cost exceeded budget threshold' as message
    FROM daily_costs
    WHERE estimated_cost_usd > 100.0
    
    UNION ALL
    
    SELECT 
        'monthly_budget_exceeded' as alert_type,
        month as cost_date,
        WAREHOUSE_NAME,
        monthly_cost_usd,
        2000.0 as budget_threshold,  -- $2000 monthly budget
        'CRITICAL' as severity,
        'Monthly cost exceeded budget threshold' as message
    FROM monthly_costs
    WHERE monthly_cost_usd > 2000.0
    
    UNION ALL
    
    SELECT 
        'cost_spike_detected' as alert_type,
        CURRENT_DATE() as cost_date,
        WAREHOUSE_NAME,
        monthly_cost_usd,
        previous_month_cost as budget_threshold,
        'MEDIUM' as severity,
        'Significant cost increase detected' as message
    FROM cost_trends
    WHERE cost_change_percent > 50.0  -- 50% increase
)

SELECT 
    'daily_costs' as metric_type,
    cost_date as measurement_date,
    WAREHOUSE_NAME as warehouse_name,
    daily_credits as credits_used,
    estimated_cost_usd as cost_usd,
    query_count,
    avg_credits_per_query,
    NULL as alert_type,
    NULL as severity,
    NULL as message
FROM daily_costs

UNION ALL

SELECT 
    'monthly_costs' as metric_type,
    month as measurement_date,
    WAREHOUSE_NAME as warehouse_name,
    monthly_credits as credits_used,
    monthly_cost_usd as cost_usd,
    NULL as query_count,
    avg_daily_credits as avg_credits_per_query,
    NULL as alert_type,
    NULL as severity,
    NULL as message
FROM monthly_costs

UNION ALL

SELECT 
    'cost_alerts' as metric_type,
    cost_date as measurement_date,
    WAREHOUSE_NAME as warehouse_name,
    NULL as credits_used,
    estimated_cost_usd as cost_usd,
    NULL as query_count,
    NULL as avg_credits_per_query,
    alert_type,
    severity,
    message
FROM cost_alerts

ORDER BY measurement_date DESC, warehouse_name
