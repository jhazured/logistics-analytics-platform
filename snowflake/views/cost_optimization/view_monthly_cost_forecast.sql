-- Monthly cost forecast view
CREATE OR REPLACE VIEW ANALYTICS.view_monthly_cost_forecast AS
WITH daily_usage AS (
    SELECT 
        DATE_TRUNC('day', start_time) AS usage_date,
        SUM(credits_used) AS daily_credits,
        SUM(credits_used) * 2.0 AS daily_cost_usd
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE())
    GROUP BY 1
),
monthly_projection AS (
    SELECT 
        DATE_TRUNC('month', CURRENT_DATE()) AS forecast_month,
        SUM(daily_credits) AS month_to_date_credits,
        SUM(daily_cost_usd) AS month_to_date_cost,
        AVG(daily_credits) AS avg_daily_credits,
        AVG(daily_cost_usd) AS avg_daily_cost,
        DAY(LAST_DAY(CURRENT_DATE())) AS days_in_month,
        DAY(CURRENT_DATE()) AS days_elapsed
    FROM daily_usage
)
SELECT 
    forecast_month,
    month_to_date_credits,
    month_to_date_cost,
    avg_daily_credits,
    avg_daily_cost,
    days_in_month,
    days_elapsed,
    
    -- Projected monthly totals
    avg_daily_credits * days_in_month as projected_monthly_credits,
    avg_daily_cost * days_in_month as projected_monthly_cost,
    
    -- Variance analysis
    (avg_daily_credits * days_in_month) - month_to_date_credits as remaining_credits,
    (avg_daily_cost * days_in_month) - month_to_date_cost as remaining_cost,
    
    -- Cost efficiency metrics
    CASE 
        WHEN days_elapsed > 0 THEN month_to_date_cost / days_elapsed
        ELSE 0
    END as cost_per_day,
    
    CASE 
        WHEN month_to_date_credits > 0 THEN month_to_date_cost / month_to_date_credits
        ELSE 0
    END as cost_per_credit,
    
    -- Budget utilization
    CASE 
        WHEN projected_monthly_cost > 0 THEN (month_to_date_cost / projected_monthly_cost) * 100
        ELSE 0
    END as budget_utilization_pct,
    
    -- Alert thresholds
    CASE 
        WHEN projected_monthly_cost > 10000 THEN 'HIGH_COST'
        WHEN projected_monthly_cost > 5000 THEN 'MEDIUM_COST'
        ELSE 'LOW_COST'
    END as cost_category,
    
    CASE 
        WHEN (month_to_date_cost / projected_monthly_cost) > 0.8 THEN 'OVER_BUDGET_RISK'
        WHEN (month_to_date_cost / projected_monthly_cost) > 0.6 THEN 'BUDGET_WARNING'
        ELSE 'WITHIN_BUDGET'
    END as budget_status,
    
    CURRENT_TIMESTAMP() as forecast_created_at

FROM monthly_projection