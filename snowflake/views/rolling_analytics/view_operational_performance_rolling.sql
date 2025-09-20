-- Rolling Operational Performance Analytics View
-- This view provides rolling operational performance metrics

CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.ANALYTICS.V_OPERATIONAL_PERFORMANCE_ROLLING AS
WITH operational_performance_base AS (
    SELECT 
        s.shipment_date,
        s.actual_delivery_time_hours,
        s.estimated_delivery_time_hours,
        s.on_time_delivery_flag,
        s.revenue_usd,
        s.total_cost_usd,
        s.profit_margin_pct,
        s.route_efficiency_score,
        s.carbon_emissions_kg,
        s.weather_delay_minutes,
        s.traffic_delay_minutes,
        s.customer_id,
        s.vehicle_id,
        s.route_id,
        c.customer_tier,
        v.vehicle_type,
        r.route_type,
        EXTRACT(HOUR FROM s.shipment_date) as shipment_hour,
        EXTRACT(DOW FROM s.shipment_date) as shipment_day_of_week,
        EXTRACT(MONTH FROM s.shipment_date) as shipment_month
    FROM LOGISTICS_DW_PROD.MARTS.FACT_SHIPMENTS s
    JOIN LOGISTICS_DW_PROD.MARTS.DIM_CUSTOMER c ON s.customer_id = c.customer_id
    JOIN LOGISTICS_DW_PROD.MARTS.DIM_VEHICLE v ON s.vehicle_id = v.vehicle_id
    JOIN LOGISTICS_DW_PROD.MARTS.DIM_ROUTE r ON s.route_id = r.route_id
    WHERE s.shipment_date >= DATEADD('day', -365, CURRENT_DATE())
),

rolling_metrics AS (
    SELECT 
        shipment_date,
        actual_delivery_time_hours,
        estimated_delivery_time_hours,
        on_time_delivery_flag,
        revenue_usd,
        total_cost_usd,
        profit_margin_pct,
        route_efficiency_score,
        carbon_emissions_kg,
        weather_delay_minutes,
        traffic_delay_minutes,
        customer_id,
        vehicle_id,
        route_id,
        customer_tier,
        vehicle_type,
        route_type,
        shipment_hour,
        shipment_day_of_week,
        shipment_month,
        
        -- 7-day rolling metrics
        COUNT(*) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as total_shipments_7d,
        
        AVG(on_time_delivery_flag) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as overall_on_time_rate_7d,
        
        AVG(route_efficiency_score) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as overall_efficiency_score_7d,
        
        SUM(revenue_usd) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as total_revenue_7d,
        
        SUM(total_cost_usd) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as total_cost_7d,
        
        AVG(profit_margin_pct) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_profit_margin_7d,
        
        AVG(weather_delay_minutes) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_weather_delay_7d,
        
        AVG(traffic_delay_minutes) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_traffic_delay_7d,
        
        -- 30-day rolling metrics
        COUNT(*) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as total_shipments_30d,
        
        AVG(on_time_delivery_flag) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as overall_on_time_rate_30d,
        
        AVG(route_efficiency_score) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as overall_efficiency_score_30d,
        
        SUM(revenue_usd) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as total_revenue_30d,
        
        SUM(total_cost_usd) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as total_cost_30d,
        
        AVG(profit_margin_pct) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as avg_profit_margin_30d,
        
        AVG(weather_delay_minutes) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as avg_weather_delay_30d,
        
        AVG(traffic_delay_minutes) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as avg_traffic_delay_30d,
        
        -- 90-day rolling metrics
        COUNT(*) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as total_shipments_90d,
        
        AVG(on_time_delivery_flag) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as overall_on_time_rate_90d,
        
        AVG(route_efficiency_score) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as overall_efficiency_score_90d,
        
        SUM(revenue_usd) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as total_revenue_90d,
        
        SUM(total_cost_usd) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as total_cost_90d,
        
        AVG(profit_margin_pct) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as avg_profit_margin_90d,
        
        AVG(weather_delay_minutes) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as avg_weather_delay_90d,
        
        AVG(traffic_delay_minutes) OVER (
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as avg_traffic_delay_90d,
        
        -- Trend analysis
        LAG(on_time_delivery_flag, 7) OVER (
            ORDER BY shipment_date
        ) as on_time_rate_7d_ago,
        
        LAG(revenue_usd, 7) OVER (
            ORDER BY shipment_date
        ) as revenue_7d_ago,
        
        LAG(profit_margin_pct, 7) OVER (
            ORDER BY shipment_date
        ) as profit_margin_7d_ago
        
    FROM operational_performance_base
)

SELECT 
    shipment_date,
    actual_delivery_time_hours,
    estimated_delivery_time_hours,
    on_time_delivery_flag,
    revenue_usd,
    total_cost_usd,
    profit_margin_pct,
    route_efficiency_score,
    carbon_emissions_kg,
    weather_delay_minutes,
    traffic_delay_minutes,
    customer_id,
    vehicle_id,
    route_id,
    customer_tier,
    vehicle_type,
    route_type,
    shipment_hour,
    shipment_day_of_week,
    shipment_month,
    
    -- 7-day metrics
    total_shipments_7d,
    overall_on_time_rate_7d,
    overall_efficiency_score_7d,
    total_revenue_7d,
    total_cost_7d,
    avg_profit_margin_7d,
    avg_weather_delay_7d,
    avg_traffic_delay_7d,
    
    -- 30-day metrics
    total_shipments_30d,
    overall_on_time_rate_30d,
    overall_efficiency_score_30d,
    total_revenue_30d,
    total_cost_30d,
    avg_profit_margin_30d,
    avg_weather_delay_30d,
    avg_traffic_delay_30d,
    
    -- 90-day metrics
    total_shipments_90d,
    overall_on_time_rate_90d,
    overall_efficiency_score_90d,
    total_revenue_90d,
    total_cost_90d,
    avg_profit_margin_90d,
    avg_weather_delay_90d,
    avg_traffic_delay_90d,
    
    -- Trend analysis
    on_time_delivery_flag - on_time_rate_7d_ago as on_time_rate_trend_7d,
    revenue_usd - revenue_7d_ago as revenue_trend_7d,
    profit_margin_pct - profit_margin_7d_ago as profit_margin_trend_7d,
    
    -- Performance indicators
    CASE 
        WHEN overall_on_time_rate_30d > 0.95 THEN 'EXCELLENT'
        WHEN overall_on_time_rate_30d > 0.85 THEN 'GOOD'
        WHEN overall_on_time_rate_30d > 0.70 THEN 'FAIR'
        ELSE 'POOR'
    END as performance_rating,
    
    CASE 
        WHEN overall_efficiency_score_30d > 80 THEN 'HIGH_EFFICIENCY'
        WHEN overall_efficiency_score_30d > 60 THEN 'MEDIUM_EFFICIENCY'
        ELSE 'LOW_EFFICIENCY'
    END as efficiency_rating,
    
    CASE 
        WHEN avg_profit_margin_30d > 15 THEN 'HIGH_PROFIT'
        WHEN avg_profit_margin_30d > 5 THEN 'MEDIUM_PROFIT'
        ELSE 'LOW_PROFIT'
    END as profit_rating,
    
    -- Risk indicators
    CASE 
        WHEN overall_on_time_rate_30d < 0.70 OR overall_efficiency_score_30d < 50 OR avg_profit_margin_30d < 0 THEN 'HIGH_RISK'
        WHEN overall_on_time_rate_30d < 0.80 OR overall_efficiency_score_30d < 60 OR avg_profit_margin_30d < 5 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END as risk_level,
    
    -- Operational efficiency indicators
    CASE 
        WHEN avg_weather_delay_30d > 60 THEN 'HIGH_WEATHER_IMPACT'
        WHEN avg_weather_delay_30d > 30 THEN 'MEDIUM_WEATHER_IMPACT'
        ELSE 'LOW_WEATHER_IMPACT'
    END as weather_impact_level,
    
    CASE 
        WHEN avg_traffic_delay_30d > 45 THEN 'HIGH_TRAFFIC_IMPACT'
        WHEN avg_traffic_delay_30d > 20 THEN 'MEDIUM_TRAFFIC_IMPACT'
        ELSE 'LOW_TRAFFIC_IMPACT'
    END as traffic_impact_level

FROM rolling_metrics
WHERE shipment_date >= DATEADD('day', -90, CURRENT_DATE())
ORDER BY shipment_date DESC
