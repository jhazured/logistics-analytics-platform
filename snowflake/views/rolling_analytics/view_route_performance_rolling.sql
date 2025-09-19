-- 8. Route Performance Rolling
-- File: models/analytics/rolling_analytics/view_route_performance_rolling.sql
{{ config(
    materialized='table',
    tags=['analytics', 'rolling', 'routes']
) }}

WITH route_daily_performance AS (
    SELECT 
        fs.route_id,
        fs.shipment_date,
        dr.route_name,
        dr.route_type,
        {{ classify_haul_type('dr.total_distance_km') }} AS haul_type,
        dl_origin.city AS origin_city,
        
        -- Daily route metrics
        COUNT(*) AS daily_trips,
        AVG(fs.actual_duration_minutes) AS avg_actual_duration,
        AVG(fs.planned_duration_minutes) AS avg_planned_duration,
        AVG(fs.actual_duration_minutes / NULLIF(fs.planned_duration_minutes, 1)) AS avg_duration_ratio,
        AVG(CASE WHEN fs.is_on_time THEN 1.0 ELSE 0.0 END) AS daily_on_time_rate,
        AVG(fs.customer_rating) AS avg_customer_satisfaction,
        SUM(fs.fuel_cost) AS total_fuel_cost,
        SUM(fs.delivery_cost) AS total_delivery_cost,
        SUM(fs.revenue) AS total_revenue,
        AVG(fs.fuel_cost / NULLIF(fs.distance_km, 0)) AS avg_fuel_cost_per_km,
        
        -- Operational challenges
        SUM(CASE WHEN fs.actual_duration_minutes > fs.planned_duration_minutes * 1.5 THEN 1 ELSE 0 END) AS severe_delays,
        SUM(CASE WHEN fs.customer_rating < 7 THEN 1 ELSE 0 END) AS poor_ratings
        
    FROM {{ ref('fact_shipments') }} fs
    JOIN {{ ref('dim_route') }} dr ON fs.route_id = dr.route_id
    JOIN {{ ref('dim_location') }} dl_origin ON fs.origin_location_id = dl_origin.location_id
    WHERE fs.shipment_date >= CURRENT_DATE() - 365
        AND fs.is_delivered = TRUE
    GROUP BY 1,2,3,4,5,6
)

SELECT 
    route_id,
    shipment_date,
    route_name,
    route_type,
    haul_type,
    origin_city,
    
    -- Current performance
    daily_trips,
    avg_actual_duration,
    avg_planned_duration,
    avg_duration_ratio,
    daily_on_time_rate,
    avg_customer_satisfaction,
    total_fuel_cost,
    total_revenue,
    avg_fuel_cost_per_km,
    severe_delays,
    poor_ratings,
    
    -- 7-day rolling metrics
    {{ rolling_average('daily_on_time_rate', 'route_id', 'shipment_date', [7]) }} AS on_time_rate_7d_avg,
    {{ rolling_average('avg_duration_ratio', 'route_id', 'shipment_date', [7]) }} AS duration_ratio_7d_avg,
    {{ rolling_average('avg_customer_satisfaction', 'route_id', 'shipment_date', [7]) }} AS satisfaction_7d_avg,
    {{ rolling_average('avg_fuel_cost_per_km', 'route_id', 'shipment_date', [7]) }} AS fuel_efficiency_7d_avg,
    
    -- 30-day rolling metrics  
    {{ rolling_average('daily_on_time_rate', 'route_id', 'shipment_date', [30]) }} AS on_time_rate_30d_avg,
    {{ rolling_average('avg_duration_ratio', 'route_id', 'shipment_date', [30]) }} AS duration_ratio_30d_avg,
    {{ rolling_average('avg_customer_satisfaction', 'route_id', 'shipment_date', [30]) }} AS satisfaction_30d_avg,
    
    -- 90-day rolling metrics
    {{ rolling_average('daily_on_time_rate', 'route_id', 'shipment_date', [90]) }} AS on_time_rate_90d_avg,
    {{ rolling_average('avg_duration_ratio', 'route_id', 'shipment_date', [90]) }} AS duration_ratio_90d_avg,
    {{ rolling_average('avg_customer_satisfaction', 'route_id', 'shipment_date', [90]) }} AS satisfaction_90d_avg,
    
    -- Volatility measures
    {{ calculate_volatility('daily_on_time_rate', 'route_id', 'shipment_date', 30) }} AS on_time_volatility_30d,
    {{ calculate_volatility('avg_duration_ratio', 'route_id', 'shipment_date', 30) }} AS duration_volatility_30d,
    
    -- Trend indicators
    {{ calculate_trend('on_time_rate_7d_avg', 'on_time_rate_30d_avg') }} AS performance_trend_7d_vs_30d,
    {{ calculate_trend('duration_ratio_7d_avg', 'duration_ratio_30d_avg') }} AS efficiency_trend_7d_vs_30d,
    {{ calculate_trend('satisfaction_7d_avg', 'satisfaction_30d_avg') }} AS satisfaction_trend_7d_vs_30d,
    
    -- Performance classification
    CASE 
        WHEN on_time_rate_30d_avg >= 0.95 AND satisfaction_30d_avg >= 8.5 THEN 'excellent'
        WHEN on_time_rate_30d_avg >= 0.90 AND satisfaction_30d_avg >= 8.0 THEN 'good'
        WHEN on_time_rate_30d_avg >= 0.80 AND satisfaction_30d_avg >= 7.0 THEN 'acceptable'
        ELSE 'needs_improvement'
    END AS route_performance_rating,
    
    -- Risk assessment
    CASE 
        WHEN on_time_volatility_30d > 0.3 OR duration_volatility_30d > 0.4 THEN 'high_risk'
        WHEN on_time_volatility_30d > 0.2 OR duration_volatility_30d > 0.25 THEN 'medium_risk'
        ELSE 'low_risk'
    END AS route_risk_level

FROM route_daily_performance