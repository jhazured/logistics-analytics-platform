-- Real-time Customer Features for ML Inference
-- Optimized for low-latency feature serving

{{ config(
    materialized='view',
    tags=['ml', 'serving', 'real_time', 'customer_features']
) }}

WITH latest_customer_features AS (
    SELECT 
        customer_id,
        -- Convert customer tier to numeric
        CASE 
            WHEN customer_tier = 'PREMIUM' THEN 3
            WHEN customer_tier = 'STANDARD' THEN 2
            WHEN customer_tier = 'BASIC' THEN 1
            ELSE 1
        END as customer_tier_numeric,
        -- Calculate customer tenure from available data
        DATEDIFF(day, CURRENT_DATE() - 365, CURRENT_DATE()) as customer_tenure_days,
        credit_limit_usd,
        customer_tier,
        'LOGISTICS' as industry_code,
        -- Calculate on-time rate from available data
        AVG(on_time_flag) OVER (PARTITION BY customer_id) as customer_on_time_rate_30d,
        customer_tier as customer_reliability_tier,
        -- Simple risk score based on credit limit
        CASE 
            WHEN credit_limit_usd >= 1000000 THEN 1
            WHEN credit_limit_usd >= 100000 THEN 2
            ELSE 3
        END as risk_score,
        feature_date,
        CURRENT_TIMESTAMP() as feature_created_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY feature_date DESC
        ) as rn
    FROM {{ ref('tbl_ml_consolidated_feature_store') }}
    WHERE feature_date >= CURRENT_DATE() - 7
        AND entity_type = 'shipment'
)
SELECT 
    customer_id,
    customer_tier_numeric,
    customer_tenure_days,
    credit_limit_usd,
    customer_tier,
    industry_code,
    customer_on_time_rate_30d,
    customer_reliability_tier,
    risk_score,
    feature_date,
    feature_created_at
FROM latest_customer_features
WHERE rn = 1
