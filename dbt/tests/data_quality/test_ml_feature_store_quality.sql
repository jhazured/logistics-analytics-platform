-- Test ML Feature Store data quality
-- Validates that ML features are properly calculated and within expected ranges

WITH ml_feature_validation AS (
    SELECT 
        shipment_id,
        customer_id,
        vehicle_id,
        route_id,
        shipment_date,
        
        -- Customer features validation
        customer_tier_numeric,
        customer_tenure_days,
        credit_limit_usd,
        
        -- Vehicle features validation
        vehicle_age_years,
        vehicle_type_numeric,
        capacity_kg,
        fuel_efficiency_mpg,
        current_mileage,
        
        -- Route features validation
        distance_km,
        estimated_travel_time_hours,
        route_type_numeric,
        
        -- Shipment features validation
        actual_delivery_time_hours,
        estimated_delivery_time_hours,
        delivery_time_variance_hours,
        is_delayed,
        on_time_delivery_flag,
        revenue,
        total_cost,
        profit_margin_pct,
        route_efficiency_score,
        carbon_emissions_kg,
        weather_delay_minutes,
        traffic_delay_minutes,
        
        -- Rolling averages validation
        customer_on_time_rate_30d,
        route_efficiency_30d,
        vehicle_profit_margin_30d,
        
        -- Feature engineering validation
        customer_reliability_tier,
        risk_score,
        feature_hash,
        feature_created_at
        
    FROM {{ ref('ml_consolidated_feature_store') }}
    WHERE shipment_date >= DATEADD('day', -90, CURRENT_DATE())
),
feature_quality_checks AS (
    SELECT 
        *,
        
        -- Validate customer tier numeric (should be 1-3)
        CASE 
            WHEN customer_tier_numeric NOT IN (1, 2, 3) THEN 1 
            ELSE 0 
        END as invalid_customer_tier,
        
        -- Validate vehicle age (should be reasonable)
        CASE 
            WHEN vehicle_age_years < 0 OR vehicle_age_years > 50 THEN 1 
            ELSE 0 
        END as invalid_vehicle_age,
        
        -- Validate rolling averages (should be between 0 and 1)
        CASE 
            WHEN customer_on_time_rate_30d < 0 OR customer_on_time_rate_30d > 1 THEN 1 
            ELSE 0 
        END as invalid_on_time_rate,
        
        -- Validate route efficiency (should be between 0 and 100)
        CASE 
            WHEN route_efficiency_score < 0 OR route_efficiency_score > 100 THEN 1 
            ELSE 0 
        END as invalid_route_efficiency,
        
        -- Validate risk score (should be between 0 and 1)
        CASE 
            WHEN risk_score < 0 OR risk_score > 1 THEN 1 
            ELSE 0 
        END as invalid_risk_score,
        
        -- Validate feature hash (should not be null)
        CASE 
            WHEN feature_hash IS NULL THEN 1 
            ELSE 0 
        END as missing_feature_hash,
        
        -- Validate feature created timestamp
        CASE 
            WHEN feature_created_at IS NULL THEN 1 
            ELSE 0 
        END as missing_feature_timestamp
        
    FROM ml_feature_validation
)
SELECT 
    shipment_id,
    customer_id,
    vehicle_id,
    route_id,
    shipment_date,
    customer_tier_numeric,
    vehicle_age_years,
    customer_on_time_rate_30d,
    route_efficiency_score,
    risk_score,
    feature_hash,
    feature_created_at,
    invalid_customer_tier,
    invalid_vehicle_age,
    invalid_on_time_rate,
    invalid_route_efficiency,
    invalid_risk_score,
    missing_feature_hash,
    missing_feature_timestamp
FROM feature_quality_checks
WHERE 
    invalid_customer_tier = 1
    OR invalid_vehicle_age = 1
    OR invalid_on_time_rate = 1
    OR invalid_route_efficiency = 1
    OR invalid_risk_score = 1
    OR missing_feature_hash = 1
    OR missing_feature_timestamp = 1
