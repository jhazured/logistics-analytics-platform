-- Customer Behavior Segments View
-- This view references the dbt model to ensure consistency and eliminate redundancy
-- Updated to use dbt model as single source of truth

CREATE OR REPLACE VIEW ANALYTICS.view_customer_behavior_segments AS
SELECT 
    customer_id,
    customer_name,
    customer_type,
    industry,
    volume_segment,
    service_level,
    
    -- RFM Scores
    recency_score,
    frequency_score,
    monetary_score,
    rfm_segment,
    
    -- Shipping behavior
    total_shipments,
    avg_monthly_shipments,
    avg_shipment_weight,
    avg_shipment_volume,
    avg_shipping_distance,
    
    -- Financial metrics
    total_revenue,
    avg_order_value,
    total_profit,
    profit_margin_rate,
    
    -- Service quality
    on_time_rate,
    avg_customer_rating,
    delivery_reliability,
    satisfaction_level,
    
    -- Preferences and patterns
    preferred_delivery_window,
    delivery_flexibility_score,
    preferred_priority,
    unique_destinations,
    preferred_day_of_week,
    preferred_hour,
    shipping_predictability,
    
    -- Seasonal behavior
    summer_activity_rate,
    winter_activity_rate,
    weekend_activity_rate,
    
    -- Activity status
    days_since_last_shipment,
    activity_status,
    
    -- Overall segmentation
    value_volume_segment,
    
    -- ML features for churn prediction
    churn_risk_days,
    expected_vs_actual_frequency,
    
    -- Customer lifetime value prediction features
    revenue_per_shipment,
    shipments_per_month

FROM LOGISTICS_DW_PROD.MARTS.ML_CUSTOMER_BEHAVIOR_SEGMENTS
ORDER BY total_revenue DESC