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
    month_to_date