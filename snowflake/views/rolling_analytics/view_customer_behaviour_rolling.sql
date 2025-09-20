-- Rolling Customer Behavior Analytics View
-- This view references the dbt model to ensure consistency and eliminate redundancy
-- Updated to use dbt model as single source of truth

CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.ANALYTICS.V_CUSTOMER_BEHAVIOUR_ROLLING AS
SELECT 
    customer_id,
    shipment_date,
    customer_name,
    volume_segment,
    customer_type,
    
    -- Current day metrics
    daily_shipments,
    daily_weight,
    daily_volume,
    daily_revenue,
    daily_avg_rating,
    daily_on_time_rate,
    
    -- Rolling metrics
    shipments_30d_avg,
    revenue_30d_avg,
    weight_30d_avg,
    shipments_30d_total,
    revenue_30d_total,
    
    -- Year-over-year comparison
    shipments_same_day_last_year,
    revenue_same_day_last_year,
    revenue_yoy_growth_percent,
    
    -- Trend indicators
    shipments_7d_avg,
    shipments_90d_avg,
    activity_ratio_7d_vs_90d,
    volume_trend_7d_vs_30d,
    revenue_trend_30d_vs_90d,
    
    -- Customer lifecycle indicators
    days_since_first_order,
    days_to_last_order,
    
    -- Behavior consistency
    shipment_volatility_30d,
    behavior_consistency

FROM LOGISTICS_DW_PROD.MARTS.ML_CUSTOMER_BEHAVIOR_ROLLING
WHERE shipment_date >= DATEADD('day', -90, CURRENT_DATE())
ORDER BY customer_id, shipment_date DESC
