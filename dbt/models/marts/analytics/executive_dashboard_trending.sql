-- =====================================================
-- Business Intelligence Views
-- =====================================================

-- Executive Dashboard Trending
{{ config(
    materialized='view',
    tags=['bi', 'dashboard', 'executive']
) }}

WITH daily_kpis AS (
    SELECT 
        fs.shipment_date,
        dd.day_of_week,
        dd.season,
        dd.is_weekend,
        
        -- Volume metrics
        COUNT(*) AS daily_deliveries,
        COUNT(DISTINCT fs.customer_id) AS daily_active_customers,
        COUNT(DISTINCT fs.vehicle_id) AS daily_active_vehicles,
        
        -- Performance metrics
        AVG(CASE WHEN fs.is_on_time THEN 1.0 ELSE 0.0 END) AS daily_on_time_rate,
        AVG(fs.route_efficiency_score) AS daily_satisfaction_score,
        
        -- Financial metrics
        SUM(fs.revenue) AS daily_revenue,
        SUM(fs.delivery_cost) AS daily_delivery_cost,
        SUM(fs.fuel_cost) AS daily_fuel_cost,
        SUM(fs.revenue - fs.delivery_cost - fs.fuel_cost) AS daily_profit,
        
        -- Efficiency metrics
        SUM(fs.distance_km) AS daily_distance,
        AVG(fs.weight_kg / NULLIF(dv.capacity_kg, 1)) AS daily_capacity_utilization,
        SUM(fs.distance_km) / NULLIF(SUM(fs.actual_duration_minutes), 0) * 60 AS daily_avg_speed
        
    FROM {{ ref('fact_shipments') }} fs
    JOIN {{ ref('dim_date') }} dd ON to_date(fs.shipment_date) = dd.date
    JOIN {{ ref('dim_vehicle') }} dv ON fs.vehicle_id = dv.vehicle_id
    WHERE fs.shipment_date >= CURRENT_DATE() - 365
        AND fs.is_delivered = TRUE
    GROUP BY 1,2,3,4
),

rolling_metrics AS (
    SELECT 
        *,
        -- 7-day rolling averages
        AVG(daily_deliveries) OVER (ORDER BY shipment_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS deliveries_7d_avg,
        AVG(daily_on_time_rate) OVER (ORDER BY shipment_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS on_time_rate_7d_avg,
        AVG(daily_satisfaction_score) OVER (ORDER BY shipment_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS satisfaction_7d_avg,
        AVG(daily_revenue) OVER (ORDER BY shipment_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS revenue_7d_avg,
        AVG(daily_profit) OVER (ORDER BY shipment_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS profit_7d_avg,
        
        -- 30-day rolling averages
        AVG(daily_deliveries) OVER (ORDER BY shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS deliveries_30d_avg,
        AVG(daily_on_time_rate) OVER (ORDER BY shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS on_time_rate_30d_avg,
        AVG(daily_satisfaction_score) OVER (ORDER BY shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS satisfaction_30d_avg,
        AVG(daily_revenue) OVER (ORDER BY shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS revenue_30d_avg,
        
        -- Same day of week last week
        LAG(daily_deliveries, 7) OVER (ORDER BY shipment_date) AS deliveries_same_dow_last_week,
        LAG(daily_on_time_rate, 7) OVER (ORDER BY shipment_date) AS on_time_rate_same_dow_last_week,
        LAG(daily_revenue, 7) OVER (ORDER BY shipment_date) AS revenue_same_dow_last_week,
        
        -- Same date last year
        LAG(daily_deliveries, 365) OVER (ORDER BY shipment_date) AS deliveries_same_date_last_year,
        LAG(daily_revenue, 365) OVER (ORDER BY shipment_date) AS revenue_same_date_last_year
        
    FROM daily_kpis
)

SELECT 
    shipment_date,
    day_of_week,
    season,
    is_weekend,
    
    -- Current period performance
    daily_deliveries,
    daily_active_customers,
    daily_active_vehicles,
    ROUND(daily_on_time_rate * 100, 1) AS on_time_percentage,
    ROUND(daily_satisfaction_score, 2) AS satisfaction_score,
    ROUND(daily_revenue, 2) AS revenue,
    ROUND(daily_profit, 2) AS profit,
    ROUND(daily_profit / NULLIF(daily_revenue, 0) * 100, 1) AS profit_margin_percentage,
    
    -- Rolling context
    ROUND(deliveries_7d_avg, 1) AS deliveries_7d_avg,
    ROUND(on_time_rate_7d_avg * 100, 1) AS on_time_7d_avg_percentage,
    ROUND(satisfaction_7d_avg, 2) AS satisfaction_7d_avg,
    ROUND(revenue_7d_avg, 2) AS revenue_7d_avg,
    ROUND(profit_7d_avg, 2) AS profit_7d_avg,
    
    -- Week-over-week comparisons
    ROUND((daily_deliveries - deliveries_same_dow_last_week) / NULLIF(deliveries_same_dow_last_week, 0) * 100, 1) AS deliveries_wow_change_percent,
    ROUND((daily_on_time_rate - on_time_rate_same_dow_last_week) * 100, 1) AS on_time_wow_change_points,
    ROUND((daily_revenue - revenue_same_dow_last_week) / NULLIF(revenue_same_dow_last_week, 0) * 100, 1) AS revenue_wow_change_percent,
    
    -- Year-over-year comparisons
    ROUND((daily_deliveries - deliveries_same_date_last_year) / NULLIF(deliveries_same_date_last_year, 0) * 100, 1) AS deliveries_yoy_change_percent,
    ROUND((daily_revenue - revenue_same_date_last_year) / NULLIF(revenue_same_date_last_year, 0) * 100, 1) AS revenue_yoy_change_percent,
    
    -- Trend classification
    CASE 
        WHEN deliveries_7d_avg > deliveries_30d_avg * 1.05 THEN 'increasing'
        WHEN deliveries_7d_avg < deliveries_30d_avg * 0.95 THEN 'decreasing'
        ELSE 'stable'
    END AS volume_trend,
    CASE 
        WHEN on_time_rate_7d_avg > on_time_rate_30d_avg * 1.02 THEN 'improving'
        WHEN on_time_rate_7d_avg < on_time_rate_30d_avg * 0.98 THEN 'declining'
        ELSE 'stable'
    END AS performance_trend,
    CASE 
        WHEN revenue_7d_avg > revenue_30d_avg * 1.05 THEN 'growing'
        WHEN revenue_7d_avg < revenue_30d_avg * 0.95 THEN 'declining'
        ELSE 'stable'
    END AS revenue_trend,
    
    -- Performance vs benchmarks
    CASE 
        WHEN daily_on_time_rate >= 0.95 THEN 'excellent'
        WHEN daily_on_time_rate >= 0.90 THEN 'good'
        WHEN daily_on_time_rate >= 0.80 THEN 'acceptable'
        ELSE 'needs_improvement'
    END AS performance_rating,
    
    CASE 
        WHEN daily_satisfaction_score >= 9 THEN 'excellent'
        WHEN daily_satisfaction_score >= 8 THEN 'good'
        WHEN daily_satisfaction_score >= 7 THEN 'acceptable'
        ELSE 'needs_improvement'
    END AS satisfaction_rating,
    
    -- Seasonal and cyclical patterns
    CASE 
        WHEN is_weekend THEN 'weekend'
        WHEN day_of_week = 1 THEN 'monday'
        WHEN day_of_week = 5 THEN 'friday'
        ELSE 'midweek'
    END AS day_type_pattern,
    
    -- Alert indicators
    CASE 
        WHEN daily_on_time_rate < 0.8 THEN 'performance_alert'
        WHEN daily_satisfaction_score < 7 THEN 'satisfaction_alert'
        WHEN (daily_revenue - revenue_7d_avg) / NULLIF(revenue_7d_avg, 0) < -0.2 THEN 'revenue_alert'
        ELSE 'normal'
    END AS alert_status

FROM rolling_metrics
WHERE shipment_date >= CURRENT_DATE() - 90  -- Last 90 days for dashboard