-- =====================================================
-- Rolling Window Analytics Views
-- =====================================================

-- Operational Performance Rolling
{{ config(
    materialized='table',
    tags=['analytics', 'rolling', 'performance']
) }}

WITH daily_performance AS (
    SELECT 
        fs.vehicle_id,
        fs.shipment_date,
        dv.vehicle_type,
        dl_origin.city AS origin_city,
        
        -- Daily aggregations
        COUNT(*) AS daily_deliveries,
        SUM(fs.distance_km) AS daily_distance_km,
        SUM(fs.fuel_cost) AS daily_fuel_cost,
        SUM(fs.delivery_cost) AS daily_delivery_cost,
        SUM(fs.revenue) AS daily_revenue,
        AVG(fs.customer_rating) AS daily_avg_rating,
        AVG(CASE WHEN fs.is_on_time THEN 1.0 ELSE 0.0 END) AS daily_on_time_rate,
        
        -- Efficiency metrics
        SUM(fs.distance_km) / NULLIF(SUM(fs.actual_duration_minutes), 0) * 60 AS daily_avg_speed_kmh,
        SUM(fs.fuel_cost) / NULLIF(SUM(fs.distance_km), 0) AS daily_fuel_cost_per_km,
        SUM(fs.revenue - fs.delivery_cost - fs.fuel_cost) AS daily_profit
        
    FROM {{ ref('fact_shipments') }} fs
    JOIN {{ ref('dim_vehicle') }} dv ON fs.vehicle_id = dv.vehicle_id
    JOIN {{ ref('dim_location') }} dl_origin ON fs.origin_location_id = dl_origin.location_id
    WHERE fs.is_delivered = TRUE
        AND fs.shipment_date >= CURRENT_DATE() - 365
    GROUP BY 1, 2, 3, 4
)

SELECT 
    vehicle_id,
    shipment_date,
    vehicle_type,
    origin_city,
    
    -- Current day metrics
    daily_deliveries,
    daily_distance_km,
    daily_fuel_cost,
    daily_delivery_cost,
    daily_revenue,
    daily_avg_rating,
    daily_on_time_rate,
    daily_avg_speed_kmh,
    daily_fuel_cost_per_km,
    daily_profit,
    
    -- 7-day rolling averages
    {{ rolling_average('daily_deliveries', 'vehicle_id', 'shipment_date', [7]) }} AS deliveries_7d_avg,
    {{ rolling_average('daily_distance_km', 'vehicle_id', 'shipment_date', [7]) }} AS distance_7d_avg,
    {{ rolling_average('daily_on_time_rate', 'vehicle_id', 'shipment_date', [7]) }} AS on_time_rate_7d_avg,
    {{ rolling_average('daily_avg_rating', 'vehicle_id', 'shipment_date', [7]) }} AS rating_7d_avg,
    {{ rolling_average('daily_fuel_cost_per_km', 'vehicle_id', 'shipment_date', [7]) }} AS fuel_efficiency_7d_avg,
    
    -- 30-day rolling averages
    {{ rolling_average('daily_deliveries', 'vehicle_id', 'shipment_date', [30]) }} AS deliveries_30d_avg,
    {{ rolling_average('daily_distance_km', 'vehicle_id', 'shipment_date', [30]) }} AS distance_30d_avg,
    {{ rolling_average('daily_on_time_rate', 'vehicle_id', 'shipment_date', [30]) }} AS on_time_rate_30d_avg,
    {{ rolling_average('daily_avg_rating', 'vehicle_id', 'shipment_date', [30]) }} AS rating_30d_avg,
    {{ rolling_average('daily_fuel_cost_per_km', 'vehicle_id', 'shipment_date', [30]) }} AS fuel_efficiency_30d_avg,
    
    -- 90-day rolling averages
    {{ rolling_average('daily_deliveries', 'vehicle_id', 'shipment_date', [90]) }} AS deliveries_90d_avg,
    {{ rolling_average('daily_distance_km', 'vehicle_id', 'shipment_date', [90]) }} AS distance_90d_avg,
    {{ rolling_average('daily_on_time_rate', 'vehicle_id', 'shipment_date', [90]) }} AS on_time_rate_90d_avg,
    {{ rolling_average('daily_avg_rating', 'vehicle_id', 'shipment_date', [90]) }} AS rating_90d_avg,
    {{ rolling_average('daily_fuel_cost_per_km', 'vehicle_id', 'shipment_date', [90]) }} AS fuel_efficiency_90d_avg,
    
    -- Performance trend indicators
    {{ calculate_trend('daily_on_time_rate', 'on_time_rate_7d_avg') }} AS on_time_trend_7d,
    {{ calculate_trend('daily_fuel_cost_per_km', 'fuel_efficiency_7d_avg') }} AS fuel_efficiency_trend_7d,
    {{ calculate_trend('daily_avg_rating', 'rating_7d_avg') }} AS rating_trend_7d,
    
    -- Volatility measures
    {{ calculate_volatility('daily_on_time_rate', 'vehicle_id', 'shipment_date', 30) }} AS on_time_volatility_30d,
    {{ calculate_volatility('daily_fuel_cost_per_km', 'vehicle_id', 'shipment_date', 30) }} AS fuel_efficiency_volatility_30d,
    
    -- Performance degradation indicators
    CASE 
        WHEN on_time_rate_7d_avg < on_time_rate_30d_avg * 0.9 THEN 'degrading'
        WHEN on_time_rate_7d_avg > on_time_rate_30d_avg * 1.1 THEN 'improving'
        ELSE 'stable'
    END AS performance_trend_7d_vs_30d,
    
    CASE 
        WHEN fuel_efficiency_7d_avg > fuel_efficiency_30d_avg * 1.1 THEN 'degrading'
        WHEN fuel_efficiency_7d_avg < fuel_efficiency_30d_avg * 0.9 THEN 'improving'
        ELSE 'stable'
    END AS fuel_efficiency_trend_7d_vs_30d

FROM daily_performance