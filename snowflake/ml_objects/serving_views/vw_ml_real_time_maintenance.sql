-- Real-time Maintenance Features for ML Inference
-- Optimized for predictive maintenance model serving

CREATE OR REPLACE VIEW ML_SERVING.REAL_TIME_MAINTENANCE_FEATURES AS
WITH latest_maintenance_features AS (
    SELECT 
        vehicle_id,
        feature_date,
        vehicle_type,
        make,
        model,
        model_year,
        current_mileage,
        maintenance_interval_miles,
        fuel_efficiency_mpg,
        
        -- Telemetry features
        avg_engine_temp,
        avg_fuel_level,
        avg_engine_rpm,
        total_harsh_braking,
        total_harsh_acceleration,
        total_speeding_events,
        avg_idle_time,
        avg_engine_health,
        telemetry_records_count,
        
        -- Maintenance history
        days_since_last_maintenance,
        miles_since_last_maintenance,
        last_maintenance_cost,
        last_maintenance_type,
        
        -- Rolling metrics
        rolling_30d_maintenance_count,
        rolling_30d_maintenance_cost,
        rolling_30d_avg_duration,
        rolling_30d_avg_risk_score,
        
        rolling_90d_maintenance_count,
        rolling_90d_maintenance_cost,
        rolling_90d_avg_duration,
        rolling_90d_avg_risk_score,
        
        -- ML features
        maintenance_frequency_30d_category,
        maintenance_risk_level_30d,
        avg_cost_per_maintenance_90d,
        predictive_maintenance_score,
        maintenance_efficiency_score,
        
        -- Target variables
        next_maintenance_due_days,
        maintenance_risk_score,
        is_maintenance_due,
        
        -- Metadata
        feature_created_at,
        feature_version,
        
        ROW_NUMBER() OVER (
            PARTITION BY vehicle_id 
            ORDER BY feature_date DESC, feature_created_at DESC
        ) as rn
        
    FROM ML_FEATURES.MAINTENANCE_FEATURES
    WHERE is_serving_data = TRUE
        AND feature_date >= CURRENT_DATE() - 3  -- Last 3 days for maintenance
)
SELECT 
    vehicle_id,
    feature_date,
    vehicle_type,
    make,
    model,
    model_year,
    current_mileage,
    maintenance_interval_miles,
    fuel_efficiency_mpg,
    avg_engine_temp,
    avg_fuel_level,
    avg_engine_rpm,
    total_harsh_braking,
    total_harsh_acceleration,
    total_speeding_events,
    avg_idle_time,
    avg_engine_health,
    telemetry_records_count,
    days_since_last_maintenance,
    miles_since_last_maintenance,
    last_maintenance_cost,
    last_maintenance_type,
    rolling_30d_maintenance_count,
    rolling_30d_maintenance_cost,
    rolling_30d_avg_duration,
    rolling_30d_avg_risk_score,
    rolling_90d_maintenance_count,
    rolling_90d_maintenance_cost,
    rolling_90d_avg_duration,
    rolling_90d_avg_risk_score,
    maintenance_frequency_30d_category,
    maintenance_risk_level_30d,
    avg_cost_per_maintenance_90d,
    predictive_maintenance_score,
    maintenance_efficiency_score,
    next_maintenance_due_days,
    maintenance_risk_score,
    is_maintenance_due,
    feature_created_at,
    feature_version
FROM latest_maintenance_features
WHERE rn = 1
COMMENT = 'Real-time maintenance features for predictive maintenance inference';

-- Create materialized view for better performance
CREATE OR REPLACE MATERIALIZED VIEW ML_SERVING.REAL_TIME_MAINTENANCE_FEATURES_CACHED AS
SELECT * FROM ML_SERVING.REAL_TIME_MAINTENANCE_FEATURES;

-- Refresh policy for materialized view (every 10 minutes)
ALTER MATERIALIZED VIEW ML_SERVING.REAL_TIME_MAINTENANCE_FEATURES_CACHED 
SET AUTO_REFRESH = TRUE;
