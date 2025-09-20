-- Real-time ML Feature Serving View
-- Optimized for low-latency feature serving for ML inference
-- Provides latest features for real-time predictions

CREATE OR REPLACE VIEW ML_SERVING.REAL_TIME_FEATURES AS
WITH latest_features AS (
    SELECT 
        customer_id,
        vehicle_id,
        route_id,
        feature_date,
        
        -- Customer features
        customer_tier_numeric,
        customer_tenure_days,
        credit_limit_usd,
        customer_tier,
        industry_code,
        
        -- Vehicle features
        vehicle_age_years,
        vehicle_type_numeric,
        capacity_kg,
        fuel_efficiency_mpg,
        current_mileage,
        vehicle_status,
        
        -- Route features
        distance_km,
        estimated_travel_time_hours,
        route_type_numeric,
        route_complexity_score,
        
        -- Performance features
        customer_on_time_rate_30d,
        route_efficiency_30d,
        vehicle_profit_margin_30d,
        
        -- Risk features
        customer_reliability_tier,
        risk_score,
        
        -- Metadata
        feature_created_at,
        feature_version,
        
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, vehicle_id, route_id 
            ORDER BY feature_date DESC, feature_created_at DESC
        ) as rn
        
    FROM ML_FEATURES.FEATURE_STORE
    WHERE is_serving_data = TRUE
        AND feature_date >= CURRENT_DATE() - 7  -- Last 7 days for real-time
)
SELECT 
    customer_id,
    vehicle_id,
    route_id,
    feature_date,
    customer_tier_numeric,
    customer_tenure_days,
    credit_limit_usd,
    customer_tier,
    industry_code,
    vehicle_age_years,
    vehicle_type_numeric,
    capacity_kg,
    fuel_efficiency_mpg,
    current_mileage,
    vehicle_status,
    distance_km,
    estimated_travel_time_hours,
    route_type_numeric,
    route_complexity_score,
    customer_on_time_rate_30d,
    route_efficiency_30d,
    vehicle_profit_margin_30d,
    customer_reliability_tier,
    risk_score,
    feature_created_at,
    feature_version
FROM latest_features
WHERE rn = 1
COMMENT = 'Real-time ML features for inference serving';

-- Create materialized view for better performance
CREATE OR REPLACE MATERIALIZED VIEW ML_SERVING.REAL_TIME_FEATURES_CACHED AS
SELECT * FROM ML_SERVING.REAL_TIME_FEATURES;

-- Refresh policy for materialized view (every 5 minutes)
ALTER MATERIALIZED VIEW ML_SERVING.REAL_TIME_FEATURES_CACHED 
SET AUTO_REFRESH = TRUE;
