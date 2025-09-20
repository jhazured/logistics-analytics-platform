-- Real-time Vehicle Features for ML Inference
-- Optimized for predictive maintenance and vehicle optimization

{{ config(
    materialized='view',
    tags=['ml', 'serving', 'real_time', 'vehicle_features']
) }}

WITH latest_vehicle_features AS (
    SELECT 
        vehicle_id,
        vehicle_age_years,
        vehicle_type_numeric,
        capacity_kg,
        fuel_efficiency_mpg,
        current_mileage,
        vehicle_status,
        vehicle_profit_margin_30d,
        feature_date,
        feature_created_at,
        ROW_NUMBER() OVER (
            PARTITION BY vehicle_id 
            ORDER BY feature_date DESC, feature_created_at DESC
        ) as rn
    FROM {{ ref('tbl_ml_consolidated_feature_store') }}
    WHERE feature_date >= CURRENT_DATE() - 3
)
SELECT 
    vehicle_id,
    vehicle_age_years,
    vehicle_type_numeric,
    capacity_kg,
    fuel_efficiency_mpg,
    current_mileage,
    vehicle_status,
    vehicle_profit_margin_30d,
    feature_date,
    feature_created_at
FROM latest_vehicle_features
WHERE rn = 1
