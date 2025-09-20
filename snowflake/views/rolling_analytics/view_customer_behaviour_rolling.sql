-- Rolling Customer Behavior Analytics View
-- This view provides rolling behavioral metrics for customers

CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.ANALYTICS.V_CUSTOMER_BEHAVIOUR_ROLLING AS
WITH customer_behavior_base AS (
    SELECT 
        c.customer_id,
        c.customer_tier,
        c.industry_vertical,
        c.credit_limit_usd,
        c.customer_since_date,
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
        s.traffic_delay_minutes
    FROM LOGISTICS_DW_PROD.MARTS.DIM_CUSTOMER c
    JOIN LOGISTICS_DW_PROD.MARTS.FACT_SHIPMENTS s ON c.customer_id = s.customer_id
    WHERE s.shipment_date >= DATEADD('day', -365, CURRENT_DATE())
        AND c.is_active = true
),

rolling_metrics AS (
    SELECT 
        customer_id,
        customer_tier,
        industry_vertical,
        credit_limit_usd,
        customer_since_date,
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
        
        -- 7-day rolling metrics
        COUNT(*) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as shipment_count_7d,
        
        AVG(on_time_delivery_flag) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as on_time_rate_7d,
        
        SUM(revenue_usd) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as total_revenue_7d,
        
        AVG(profit_margin_pct) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_profit_margin_7d,
        
        AVG(route_efficiency_score) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_efficiency_score_7d,
        
        -- 30-day rolling metrics
        COUNT(*) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as shipment_count_30d,
        
        AVG(on_time_delivery_flag) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as on_time_rate_30d,
        
        SUM(revenue_usd) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as total_revenue_30d,
        
        AVG(profit_margin_pct) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as avg_profit_margin_30d,
        
        AVG(route_efficiency_score) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as avg_efficiency_score_30d,
        
        -- 90-day rolling metrics
        COUNT(*) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as shipment_count_90d,
        
        AVG(on_time_delivery_flag) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as on_time_rate_90d,
        
        SUM(revenue_usd) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as total_revenue_90d,
        
        AVG(profit_margin_pct) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as avg_profit_margin_90d,
        
        AVG(route_efficiency_score) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as avg_efficiency_score_90d,
        
        -- Trend analysis
        LAG(on_time_delivery_flag, 7) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date
        ) as on_time_rate_7d_ago,
        
        LAG(revenue_usd, 7) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date
        ) as revenue_7d_ago,
        
        LAG(profit_margin_pct, 7) OVER (
            PARTITION BY customer_id 
            ORDER BY shipment_date
        ) as profit_margin_7d_ago
        
    FROM customer_behavior_base
)

SELECT 
    customer_id,
    customer_tier,
    industry_vertical,
    credit_limit_usd,
    customer_since_date,
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
    
    -- 7-day metrics
    shipment_count_7d,
    on_time_rate_7d,
    total_revenue_7d,
    avg_profit_margin_7d,
    avg_efficiency_score_7d,
    
    -- 30-day metrics
    shipment_count_30d,
    on_time_rate_30d,
    total_revenue_30d,
    avg_profit_margin_30d,
    avg_efficiency_score_30d,
    
    -- 90-day metrics
    shipment_count_90d,
    on_time_rate_90d,
    total_revenue_90d,
    avg_profit_margin_90d,
    avg_efficiency_score_90d,
    
    -- Trend analysis
    on_time_rate_7d - on_time_rate_7d_ago as on_time_rate_trend_7d,
    revenue_usd - revenue_7d_ago as revenue_trend_7d,
    profit_margin_pct - profit_margin_7d_ago as profit_margin_trend_7d,
    
    -- Customer behavior indicators
    CASE 
        WHEN on_time_rate_30d > 0.95 THEN 'EXCELLENT'
        WHEN on_time_rate_30d > 0.85 THEN 'GOOD'
        WHEN on_time_rate_30d > 0.70 THEN 'FAIR'
        ELSE 'POOR'
    END as reliability_rating,
    
    CASE 
        WHEN total_revenue_30d > 100000 THEN 'HIGH_VALUE'
        WHEN total_revenue_30d > 50000 THEN 'MEDIUM_VALUE'
        ELSE 'LOW_VALUE'
    END as value_rating,
    
    CASE 
        WHEN avg_profit_margin_30d > 15 THEN 'HIGH_PROFIT'
        WHEN avg_profit_margin_30d > 5 THEN 'MEDIUM_PROFIT'
        ELSE 'LOW_PROFIT'
    END as profit_rating,
    
    -- Customer lifecycle stage
    CASE 
        WHEN DATEDIFF('day', customer_since_date, CURRENT_DATE()) < 90 THEN 'NEW'
        WHEN DATEDIFF('day', customer_since_date, CURRENT_DATE()) < 365 THEN 'GROWING'
        WHEN DATEDIFF('day', customer_since_date, CURRENT_DATE()) < 1095 THEN 'ESTABLISHED'
        ELSE 'MATURE'
    END as lifecycle_stage,
    
    -- Risk indicators
    CASE 
        WHEN on_time_rate_30d < 0.70 OR total_revenue_30d < 10000 OR avg_profit_margin_30d < 0 THEN 'HIGH_RISK'
        WHEN on_time_rate_30d < 0.80 OR total_revenue_30d < 25000 OR avg_profit_margin_30d < 5 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END as risk_level,
    
    -- Churn risk indicators
    CASE 
        WHEN shipment_count_30d = 0 THEN 'HIGH_CHURN_RISK'
        WHEN shipment_count_30d < 5 THEN 'MEDIUM_CHURN_RISK'
        ELSE 'LOW_CHURN_RISK'
    END as churn_risk_level

FROM rolling_metrics
WHERE shipment_date >= DATEADD('day', -90, CURRENT_DATE())
ORDER BY customer_id, shipment_date DESC
