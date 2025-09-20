-- ML Maintenance Features Table (ML-Optimized)
-- This table is materialized from dbt model: ml_maintenance_rolling_indicators
-- Optimized for predictive maintenance ML models

CREATE OR REPLACE TABLE ML_FEATURES.MAINTENANCE_FEATURES (
    -- Primary keys
    feature_id VARCHAR(100) PRIMARY KEY,
    vehicle_id VARCHAR(20) NOT NULL,
    feature_date DATE NOT NULL,
    
    -- Vehicle characteristics
    vehicle_type VARCHAR(50),
    make VARCHAR(50),
    model VARCHAR(100),
    model_year NUMBER(4),
    current_mileage NUMBER(12,0),
    maintenance_interval_miles NUMBER(10,0),
    fuel_efficiency_mpg NUMBER(10,3),
    
    -- Telemetry features (aggregated daily)
    avg_engine_temp NUMBER(5,2),
    avg_fuel_level NUMBER(5,2),
    avg_engine_rpm NUMBER(8,0),
    total_harsh_braking NUMBER(6,0),
    total_harsh_acceleration NUMBER(6,0),
    total_speeding_events NUMBER(6,0),
    avg_idle_time NUMBER(6,2),
    avg_engine_health NUMBER(3,1),
    telemetry_records_count NUMBER(8,0),
    
    -- Maintenance history features
    days_since_last_maintenance NUMBER(6,0),
    miles_since_last_maintenance NUMBER(10,0),
    last_maintenance_cost NUMBER(10,2),
    last_maintenance_type VARCHAR(50),
    maintenance_frequency_30d NUMBER(3,0),
    maintenance_frequency_90d NUMBER(3,0),
    maintenance_frequency_365d NUMBER(3,0),
    
    -- Rolling maintenance metrics
    rolling_30d_maintenance_count NUMBER(3,0),
    rolling_30d_maintenance_cost NUMBER(12,2),
    rolling_30d_avg_duration NUMBER(6,2),
    rolling_30d_avg_risk_score NUMBER(5,2),
    
    rolling_90d_maintenance_count NUMBER(3,0),
    rolling_90d_maintenance_cost NUMBER(12,2),
    rolling_90d_avg_duration NUMBER(6,2),
    rolling_90d_avg_risk_score NUMBER(5,2),
    
    rolling_365d_maintenance_count NUMBER(3,0),
    rolling_365d_maintenance_cost NUMBER(12,2),
    rolling_365d_avg_duration NUMBER(6,2),
    rolling_365d_avg_risk_score NUMBER(5,2),
    
    -- Calculated ML features
    maintenance_frequency_30d_category VARCHAR(20),
    maintenance_risk_level_30d VARCHAR(20),
    avg_cost_per_maintenance_90d NUMBER(10,2),
    predictive_maintenance_score NUMBER(5,2),
    maintenance_efficiency_score NUMBER(5,2),
    
    -- Target variables for ML
    next_maintenance_due_days NUMBER(6,0),
    maintenance_risk_score NUMBER(5,2),
    is_maintenance_due BOOLEAN,
    
    -- Metadata
    feature_created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    feature_version VARCHAR(20) DEFAULT 'v1.0',
    model_training_date DATE,
    is_training_data BOOLEAN DEFAULT TRUE,
    is_serving_data BOOLEAN DEFAULT TRUE
)
CLUSTER BY (vehicle_id, feature_date, feature_version)
COMMENT = 'ML Maintenance Features optimized for predictive maintenance models';

-- Create indexes for ML query patterns
CREATE INDEX IF NOT EXISTS idx_ml_maintenance_vehicle_date 
ON ML_FEATURES.MAINTENANCE_FEATURES (vehicle_id, feature_date);

CREATE INDEX IF NOT EXISTS idx_ml_maintenance_risk 
ON ML_FEATURES.MAINTENANCE_FEATURES (maintenance_risk_score, feature_date);

CREATE INDEX IF NOT EXISTS idx_ml_maintenance_training 
ON ML_FEATURES.MAINTENANCE_FEATURES (is_training_data, feature_date);
