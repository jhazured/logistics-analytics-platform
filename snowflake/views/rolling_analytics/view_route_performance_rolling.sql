-- Rolling Route Performance Analytics View
-- This view provides rolling performance metrics for routes

CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.ANALYTICS.V_ROUTE_PERFORMANCE_ROLLING AS
WITH route_performance_base AS (
    SELECT 
        r.route_id,
        r.origin_location_id,
        r.destination_location_id,
        r.distance_miles,
        r.route_type,
        s.shipment_date,
        s.actual_delivery_time_hours,
        s.estimated_delivery_time_hours,
        s.on_time_delivery_flag,
        s.route_efficiency_score,
        s.revenue_usd,
        s.total_cost_usd,
        s.profit_margin_pct,
        s.carbon_emissions_kg,
        s.weather_delay_minutes,
        s.traffic_delay_minutes
    FROM LOGISTICS_DW_PROD.MARTS.DIM_ROUTE r
    JOIN LOGISTICS_DW_PROD.MARTS.FACT_SHIPMENTS s ON r.route_id = s.route_id
    WHERE s.shipment_date >= DATEADD('day', -365, CURRENT_DATE())
),

rolling_metrics AS (
    SELECT 
        route_id,
        origin_location_id,
        destination_location_id,
        distance_miles,
        route_type,
        shipment_date,
        actual_delivery_time_hours,
        estimated_delivery_time_hours,
        on_time_delivery_flag,
        route_efficiency_score,
        revenue_usd,
        total_cost_usd,
        profit_margin_pct,
        carbon_emissions_kg,
        weather_delay_minutes,
        traffic_delay_minutes,
        
        -- 7-day rolling metrics
        AVG(on_time_delivery_flag) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as on_time_rate_7d,
        
        AVG(route_efficiency_score) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as efficiency_score_7d,
        
        AVG(actual_delivery_time_hours) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_delivery_time_7d,
        
        AVG(profit_margin_pct) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_profit_margin_7d,
        
        SUM(revenue_usd) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as total_revenue_7d,
        
        AVG(weather_delay_minutes) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_weather_delay_7d,
        
        AVG(traffic_delay_minutes) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_traffic_delay_7d,
        
        -- 30-day rolling metrics
        AVG(on_time_delivery_flag) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as on_time_rate_30d,
        
        AVG(route_efficiency_score) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as efficiency_score_30d,
        
        AVG(actual_delivery_time_hours) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as avg_delivery_time_30d,
        
        AVG(profit_margin_pct) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as avg_profit_margin_30d,
        
        SUM(revenue_usd) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as total_revenue_30d,
        
        AVG(weather_delay_minutes) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as avg_weather_delay_30d,
        
        AVG(traffic_delay_minutes) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as avg_traffic_delay_30d,
        
        -- 90-day rolling metrics
        AVG(on_time_delivery_flag) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as on_time_rate_90d,
        
        AVG(route_efficiency_score) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as efficiency_score_90d,
        
        AVG(actual_delivery_time_hours) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as avg_delivery_time_90d,
        
        AVG(profit_margin_pct) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as avg_profit_margin_90d,
        
        SUM(revenue_usd) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as total_revenue_90d,
        
        AVG(weather_delay_minutes) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as avg_weather_delay_90d,
        
        AVG(traffic_delay_minutes) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as avg_traffic_delay_90d,
        
        -- Trend analysis
        LAG(on_time_delivery_flag, 7) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date
        ) as on_time_rate_7d_ago,
        
        LAG(route_efficiency_score, 7) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date
        ) as efficiency_score_7d_ago,
        
        LAG(profit_margin_pct, 7) OVER (
            PARTITION BY route_id 
            ORDER BY shipment_date
        ) as profit_margin_7d_ago
        
    FROM route_performance_base
)

SELECT 
    route_id,
    origin_location_id,
    destination_location_id,
    distance_miles,
    route_type,
    shipment_date,
    actual_delivery_time_hours,
    estimated_delivery_time_hours,
    on_time_delivery_flag,
    route_efficiency_score,
    revenue_usd,
    total_cost_usd,
    profit_margin_pct,
    carbon_emissions_kg,
    weather_delay_minutes,
    traffic_delay_minutes,
    
    -- 7-day metrics
    on_time_rate_7d,
    efficiency_score_7d,
    avg_delivery_time_7d,
    avg_profit_margin_7d,
    total_revenue_7d,
    avg_weather_delay_7d,
    avg_traffic_delay_7d,
    
    -- 30-day metrics
    on_time_rate_30d,
    efficiency_score_30d,
    avg_delivery_time_30d,
    avg_profit_margin_30d,
    total_revenue_30d,
    avg_weather_delay_30d,
    avg_traffic_delay_30d,
    
    -- 90-day metrics
    on_time_rate_90d,
    efficiency_score_90d,
    avg_delivery_time_90d,
    avg_profit_margin_90d,
    total_revenue_90d,
    avg_weather_delay_90d,
    avg_traffic_delay_90d,
    
    -- Trend analysis
    on_time_rate_7d - on_time_rate_7d_ago as on_time_rate_trend_7d,
    efficiency_score_7d - efficiency_score_7d_ago as efficiency_trend_7d,
    profit_margin_7d - profit_margin_7d_ago as profit_margin_trend_7d,
    
    -- Performance indicators
    CASE 
        WHEN on_time_rate_30d > 0.95 THEN 'EXCELLENT'
        WHEN on_time_rate_30d > 0.85 THEN 'GOOD'
        WHEN on_time_rate_30d > 0.70 THEN 'FAIR'
        ELSE 'POOR'
    END as performance_rating,
    
    CASE 
        WHEN efficiency_score_30d > 80 THEN 'HIGH_EFFICIENCY'
        WHEN efficiency_score_30d > 60 THEN 'MEDIUM_EFFICIENCY'
        ELSE 'LOW_EFFICIENCY'
    END as efficiency_rating,
    
    CASE 
        WHEN avg_profit_margin_30d > 15 THEN 'HIGH_PROFIT'
        WHEN avg_profit_margin_30d > 5 THEN 'MEDIUM_PROFIT'
        ELSE 'LOW_PROFIT'
    END as profit_rating,
    
    -- Risk indicators
    CASE 
        WHEN on_time_rate_30d < 0.70 OR efficiency_score_30d < 50 OR avg_profit_margin_30d < 0 THEN 'HIGH_RISK'
        WHEN on_time_rate_30d < 0.80 OR efficiency_score_30d < 60 OR avg_profit_margin_30d < 5 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END as risk_level

FROM rolling_metrics
WHERE shipment_date >= DATEADD('day', -90, CURRENT_DATE())
ORDER BY route_id, shipment_date DESC