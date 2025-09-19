-- 2. Customer Behavior Segments
-- File: models/analytics/ml_features/view_customer_behavior_segments.sql
{{ config(
    materialized='table',
    tags=['ml', 'features', 'customer']
) }}

WITH customer_metrics AS (
    SELECT 
        fs.customer_id,
        dc.customer_name,
        dc.customer_type,
        dc.industry,
        dc.volume_segment,
        dc.service_level,
        dc.preferred_delivery_window,
        dc.delivery_flexibility_score,
        dc.satisfaction_score,
        
        -- Shipping behavior metrics
        COUNT(*) AS total_shipments,
        COUNT(*) / NULLIF(DATEDIFF(month, MIN(fs.shipment_date), MAX(fs.shipment_date)), 0) AS avg_monthly_shipments,
        AVG(fs.weight_kg) AS avg_shipment_weight,
        AVG(fs.volume_m3) AS avg_shipment_volume,
        AVG(fs.distance_km) AS avg_shipping_distance,
        
        -- Financial metrics
        SUM(fs.revenue) AS total_revenue,
        AVG(fs.revenue) AS avg_order_value,
        SUM(fs.delivery_cost) AS total_delivery_cost,
        AVG(fs.delivery_cost) AS avg_delivery_cost,
        SUM(fs.revenue - fs.delivery_cost - fs.fuel_cost) AS total_profit,
        
        -- Performance metrics
        AVG(CASE WHEN fs.is_on_time THEN 1.0 ELSE 0.0 END) AS on_time_rate,
        AVG(fs.customer_rating) AS avg_customer_rating,
        
        -- Service preferences
        MODE() WITHIN GROUP (ORDER BY fs.priority_level) AS preferred_priority,
        COUNT(DISTINCT fs.destination_location_id) AS unique_destinations,
        
        -- Temporal patterns
        MODE() WITHIN GROUP (ORDER BY EXTRACT(dayofweek FROM fs.shipment_date)) AS preferred_day_of_week,
        MODE() WITHIN GROUP (ORDER BY EXTRACT(hour FROM fs.shipment_date)) AS preferred_hour,
        STDDEV(EXTRACT(dayofweek FROM fs.shipment_date)) AS day_of_week_variance,
        
        -- Recent activity
        MAX(fs.shipment_date) AS last_shipment_date,
        DATEDIFF(day, MAX(fs.shipment_date), CURRENT_DATE()) AS days_since_last_shipment,
        
        -- Seasonal patterns
        AVG(CASE WHEN dd.season = 'Summer' THEN 1.0 ELSE 0.0 END) AS summer_activity_rate,
        AVG(CASE WHEN dd.season = 'Winter' THEN 1.0 ELSE 0.0 END) AS winter_activity_rate,
        AVG(CASE WHEN dd.is_weekend THEN 1.0 ELSE 0.0 END) AS weekend_activity_rate
        
    FROM {{ ref('fact_shipments') }} fs
    JOIN {{ ref('dim_customer') }} dc ON fs.customer_id = dc.customer_id
    JOIN {{ ref('dim_date') }} dd ON fs.date_key = dd.date_key
    WHERE fs.shipment_date >= CURRENT_DATE() - 365  -- Last year
    GROUP BY 1,2,3,4,5,6,7,8,9
),

customer_scoring AS (
    SELECT 
        *,
        -- RFM Analysis
        CASE 
            WHEN days_since_last_shipment <= 30 THEN 5
            WHEN days_since_last_shipment <= 60 THEN 4
            WHEN days_since_last_shipment <= 90 THEN 3
            WHEN days_since_last_shipment <= 180 THEN 2
            ELSE 1
        END AS recency_score,
        
        NTILE(5) OVER (ORDER BY avg_monthly_shipments) AS frequency_score,
        NTILE(5) OVER (ORDER BY total_revenue) AS monetary_score,
        
        -- Behavior scores
        CASE 
            WHEN on_time_rate >= 0.95 THEN 'very_reliable'
            WHEN on_time_rate >= 0.85 THEN 'reliable'
            WHEN on_time_rate >= 0.70 THEN 'moderate'
            ELSE 'challenging'
        END AS delivery_reliability,
        
        CASE 
            WHEN avg_customer_rating >= 9 THEN 'very_satisfied'
            WHEN avg_customer_rating >= 7.5 THEN 'satisfied'
            WHEN avg_customer_rating >= 6 THEN 'neutral'
            ELSE 'dissatisfied'
        END AS satisfaction_level,
        
        -- Predictability scores
        CASE 
            WHEN day_of_week_variance <= 1 THEN 'very_predictable'
            WHEN day_of_week_variance <= 2 THEN 'predictable'
            WHEN day_of_week_variance <= 3 THEN 'somewhat_predictable'
            ELSE 'unpredictable'
        END AS shipping_predictability,
        
        -- Value segmentation
        CASE 
            WHEN total_revenue >= 100000 AND avg_monthly_shipments >= 20 THEN 'high_value_high_volume'
            WHEN total_revenue >= 50000 AND avg_monthly_shipments >= 10 THEN 'medium_value_medium_volume'
            WHEN total_revenue >= 10000 THEN 'medium_value_low_volume'
            ELSE 'low_value'
        END AS value_volume_segment
        
    FROM customer_metrics
)

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
    CONCAT(recency_score, frequency_score, monetary_score) AS rfm_segment,
    
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
    total_profit / NULLIF(total_revenue, 0) AS profit_margin_rate,
    
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
    CASE 
        WHEN days_since_last_shipment <= 30 THEN 'active'
        WHEN days_since_last_shipment <= 90 THEN 'moderate'
        WHEN days_since_last_shipment <= 180 THEN 'at_risk'
        ELSE 'churned'
    END AS activity_status,
    
    -- Overall segmentation
    value_volume_segment,
    
    -- ML features for churn prediction
    days_since_last_shipment AS churn_risk_days,
    (avg_monthly_shipments * 30 - days_since_last_shipment) / 30.0 AS expected_vs_actual_frequency,
    
    -- Customer lifetime value prediction features
    total_revenue / NULLIF(total_shipments, 0) AS revenue_per_shipment,
    total_shipments / NULLIF(DATEDIFF(month, 
        DATE_TRUNC('month', CURRENT_DATE() - 365), 
        DATE_TRUNC('month', CURRENT_DATE())), 0) AS shipments_per_month

FROM customer_scoring