-- Real-time Vehicle Features for ML Inference
-- Optimized for predictive maintenance and vehicle optimization

{{ config(
    materialized='view',
    tags=['ml', 'serving', 'real_time', 'vehicle_features']
) }}

WITH latest_vehicle_features AS (
    SELECT 
        vehicle_id,
        -- Calculate vehicle age from model year
        DATEDIFF(year, DATE(model_year || '-01-01'), CURRENT_DATE()) as vehicle_age_years,
        -- Convert vehicle type to numeric
        CASE 
            WHEN vehicle_type = 'TRUCK' THEN 3
            WHEN vehicle_type = 'VAN' THEN 2
            WHEN vehicle_type = 'CAR' THEN 1
            ELSE 1
        END as vehicle_type_numeric,
        capacity_kg,
        fuel_efficiency_mpg,
        current_mileage,
        vehicle_type as vehicle_status,
        -- Calculate profit margin from available data
        AVG(profit_margin) OVER (PARTITION BY vehicle_id) as vehicle_profit_margin_30d,
        feature_date,
        CURRENT_TIMESTAMP() as feature_created_at,
        ROW_NUMBER() OVER (
            PARTITION BY vehicle_id 
            ORDER BY feature_date DESC
        ) as rn
    FROM {{ ref('tbl_ml_consolidated_feature_store') }}
    WHERE feature_date >= CURRENT_DATE() - 3
        AND entity_type = 'vehicle'
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
