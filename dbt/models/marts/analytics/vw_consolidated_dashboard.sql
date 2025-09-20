-- =====================================================
-- Consolidated Business Intelligence Dashboard
-- =====================================================
-- Combines performance dashboard and executive trending functionality
-- Eliminates redundancy while providing comprehensive analytics

{{ config(
    materialized='view',
    tags=['bi', 'dashboard', 'consolidated']
) }}

WITH daily_kpis AS (
    SELECT 
        fs.shipment_date,
        dd.day_of_week,
        dd.season,
        dd.is_weekend,
        dl_origin.city AS origin_city,
        dl_origin.state AS origin_state,
        dc.volume_segment,
        dc.customer_type,
        dr.route_type,
        dv.vehicle_type,
        
        -- Volume metrics
        COUNT(*) AS daily_deliveries,
        COUNT(DISTINCT fs.customer_id) AS daily_active_customers,
        COUNT(DISTINCT fs.vehicle_id) AS daily_active_vehicles,
        
        -- Performance metrics
        {{ calculate_on_time_rate('fs.on_time_delivery_flag') }} AS daily_on_time_rate,
        AVG(fs.route_efficiency_score) AS daily_satisfaction_score,
        AVG(fs.actual_delivery_time_hours / NULLIF(fs.estimated_delivery_time_hours, 1)) AS schedule_adherence,
        
        -- Financial metrics
        SUM(fs.revenue) AS daily_revenue,
        SUM(fs.total_cost) AS daily_total_cost,
        SUM(fs.revenue - fs.total_cost) AS daily_profit,
        AVG(fs.revenue) AS avg_revenue_per_delivery,
        
        -- Efficiency metrics
        SUM(fs.distance_km) AS daily_distance,
        AVG(fs.weight_kg / NULLIF(dv.capacity_kg, 1)) AS daily_capacity_utilization,
        SUM(fs.distance_km) / NULLIF(SUM(fs.actual_delivery_time_hours), 0) AS daily_avg_speed_kmh,
        SUM(fs.total_cost) / NULLIF(SUM(fs.distance_km), 0) AS cost_per_km,
        
        -- Volume metrics
        SUM(fs.weight_kg) AS daily_weight_kg,
        SUM(fs.volume_m3) AS daily_volume_m3,
        AVG(fs.weight_kg) AS avg_shipment_weight,
        
        -- Service level metrics
        COUNT(CASE WHEN fs.priority_level = 'URGENT' THEN 1 END) AS urgent_deliveries,
        COUNT(CASE WHEN fs.service_type = 'EXPRESS' THEN 1 END) AS express_deliveries,
        AVG(CASE WHEN fs.priority_level = 'URGENT' AND fs.on_time_delivery_flag THEN 1.0 ELSE 0.0 END) AS urgent_on_time_rate
        
    FROM {{ ref('tbl_fact_shipments') }} fs
    JOIN {{ ref('tbl_dim_date') }} dd ON to_date(fs.shipment_date) = dd.date
    JOIN {{ ref('tbl_dim_location') }} dl_origin ON fs.origin_location_id = dl_origin.location_id
    JOIN {{ ref('tbl_dim_customer') }} dc ON fs.customer_id = dc.customer_id
    JOIN {{ ref('tbl_dim_route') }} dr ON fs.route_id = dr.route_id
    JOIN {{ ref('tbl_dim_vehicle') }} dv ON fs.vehicle_id = dv.vehicle_id
    WHERE fs.shipment_date >= CURRENT_DATE() - 365
        AND fs.delivery_status = 'DELIVERED'
    GROUP BY 1,2,3,4,5,6,7,8,9,10
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
        
        -- 90-day rolling averages
        AVG(daily_deliveries) OVER (ORDER BY shipment_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS deliveries_90d_avg,
        AVG(daily_on_time_rate) OVER (ORDER BY shipment_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS on_time_rate_90d_avg,
        AVG(daily_revenue) OVER (ORDER BY shipment_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS revenue_90d_avg,
        
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
    origin_city,
    origin_state,
    volume_segment,
    customer_type,
    route_type,
    vehicle_type,
    
    -- Current period performance
    daily_deliveries,
    daily_active_customers,
    daily_active_vehicles,
    ROUND(daily_on_time_rate * 100, 1) AS on_time_percentage,
    ROUND(daily_satisfaction_score, 2) AS satisfaction_score,
    ROUND(schedule_adherence * 100, 1) AS schedule_adherence_percentage,
    ROUND(daily_revenue, 2) AS revenue,
    ROUND(daily_total_cost, 2) AS total_costs,
    ROUND(daily_profit, 2) AS profit,
    ROUND(daily_profit / NULLIF(daily_revenue, 0) * 100, 1) AS profit_margin_percentage,
    ROUND(avg_revenue_per_delivery, 2) AS revenue_per_delivery,
    
    -- Efficiency metrics
    ROUND(daily_distance, 1) AS total_distance_km,
    ROUND(daily_capacity_utilization * 100, 1) AS capacity_utilization_percentage,
    ROUND(daily_avg_speed_kmh, 1) AS average_speed_kmh,
    ROUND(cost_per_km, 4) AS cost_per_km,
    
    -- Volume metrics
    ROUND(daily_weight_kg, 1) AS total_weight_kg,
    ROUND(daily_volume_m3, 2) AS total_volume_m3,
    ROUND(avg_shipment_weight, 1) AS avg_shipment_weight,
    
    -- Service level metrics
    urgent_deliveries,
    express_deliveries,
    ROUND(urgent_on_time_rate * 100, 1) AS urgent_on_time_percentage,
    
    -- Rolling context (7-day)
    ROUND(deliveries_7d_avg, 1) AS deliveries_7d_avg,
    ROUND(on_time_rate_7d_avg * 100, 1) AS on_time_7d_avg_percentage,
    ROUND(satisfaction_7d_avg, 2) AS satisfaction_7d_avg,
    ROUND(revenue_7d_avg, 2) AS revenue_7d_avg,
    ROUND(profit_7d_avg, 2) AS profit_7d_avg,
    
    -- Rolling context (30-day)
    ROUND(deliveries_30d_avg, 1) AS deliveries_30d_avg,
    ROUND(on_time_rate_30d_avg * 100, 1) AS on_time_30d_avg_percentage,
    ROUND(satisfaction_30d_avg, 2) AS satisfaction_30d_avg,
    ROUND(revenue_30d_avg, 2) AS revenue_30d_avg,
    
    -- Rolling context (90-day)
    ROUND(deliveries_90d_avg, 1) AS deliveries_90d_avg,
    ROUND(on_time_rate_90d_avg * 100, 1) AS on_time_90d_avg_percentage,
    ROUND(revenue_90d_avg, 2) AS revenue_90d_avg,
    
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
    
    -- Performance ratings
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
    
    -- Calculated performance scores
    ROUND(
        (daily_on_time_rate * 0.4) + 
        (daily_capacity_utilization * 0.3) + 
        (daily_satisfaction_score / 10.0 * 0.3), 2
    ) AS overall_performance_score,
    
    -- Productivity metrics
    ROUND(daily_deliveries / NULLIF(daily_active_vehicles, 0), 1) AS deliveries_per_vehicle,
    ROUND(daily_revenue / NULLIF(daily_active_vehicles, 0), 2) AS revenue_per_vehicle,
    ROUND(daily_distance / NULLIF(daily_active_vehicles, 0), 1) AS distance_per_vehicle,
    
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
