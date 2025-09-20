-- Real-time Customer Features for ML Inference
-- Optimized for low-latency feature serving

{{ config(
    materialized='view',
    tags=['ml', 'serving', 'real_time', 'customer_features']
) }}

WITH latest_customer_features AS (
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
        feature_created_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY feature_date DESC, feature_created_at DESC
        ) as rn
    FROM {{ ref('tbl_ml_consolidated_feature_store') }}
    WHERE feature_date >= CURRENT_DATE() - 7
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
