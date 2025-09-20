-- ML Model Registry Table
-- Tracks ML models, their performance, and deployment status

CREATE OR REPLACE TABLE LOGISTICS_DW_PROD.ML_OBJECTS.TBL_ML_MODEL_REGISTRY (
    MODEL_ID VARCHAR(50) DEFAULT UUID_STRING(),
    MODEL_NAME VARCHAR(255) NOT NULL,
    MODEL_TYPE VARCHAR(100) NOT NULL,
    MODEL_VERSION VARCHAR(50) NOT NULL,
    TRAINING_DATE TIMESTAMP_NTZ NOT NULL,
    DEPLOYMENT_DATE TIMESTAMP_NTZ,
    STATUS VARCHAR(20) NOT NULL DEFAULT 'TRAINING', -- TRAINING, ACTIVE, DEPRECATED, FAILED
    PERFORMANCE_METRICS VARIANT, -- JSON with accuracy, precision, recall, etc.
    FEATURE_COLUMNS VARIANT, -- JSON array of feature column names
    MODEL_METADATA VARIANT, -- JSON with additional model information
    TRAINING_DATA_SIZE INTEGER,
    VALIDATION_SCORE FLOAT,
    PRODUCTION_SCORE FLOAT,
    DRIFT_SCORE FLOAT,
    LAST_PREDICTION_DATE TIMESTAMP_NTZ,
    PREDICTION_COUNT INTEGER DEFAULT 0,
    CREATED_BY VARCHAR(255) DEFAULT CURRENT_USER(),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_model_registry_name ON LOGISTICS_DW_PROD.ML_OBJECTS.TBL_ML_MODEL_REGISTRY (MODEL_NAME);
CREATE INDEX IF NOT EXISTS idx_model_registry_type ON LOGISTICS_DW_PROD.ML_OBJECTS.TBL_ML_MODEL_REGISTRY (MODEL_TYPE);
CREATE INDEX IF NOT EXISTS idx_model_registry_status ON LOGISTICS_DW_PROD.ML_OBJECTS.TBL_ML_MODEL_REGISTRY (STATUS);
CREATE INDEX IF NOT EXISTS idx_model_registry_date ON LOGISTICS_DW_PROD.ML_OBJECTS.TBL_ML_MODEL_REGISTRY (TRAINING_DATE);

-- Insert sample model registry entries
INSERT INTO LOGISTICS_DW_PROD.ML_OBJECTS.TBL_ML_MODEL_REGISTRY (
    MODEL_NAME, MODEL_TYPE, MODEL_VERSION, TRAINING_DATE, STATUS,
    PERFORMANCE_METRICS, FEATURE_COLUMNS, TRAINING_DATA_SIZE, VALIDATION_SCORE
) VALUES 
(
    'route_optimization_v1',
    'route_optimization',
    '1.0.0',
    CURRENT_TIMESTAMP(),
    'ACTIVE',
    PARSE_JSON('{"accuracy": 0.85, "mae": 12.5, "mse": 156.25, "r2": 0.82}'),
    PARSE_JSON('["route_efficiency_score", "traffic_delay_factor", "weather_impact_score", "route_complexity_score", "distance_km", "fuel_efficiency_mpg", "cost_per_km", "on_time_delivery_rate", "customer_satisfaction_score"]'),
    10000,
    0.82
),
(
    'predictive_maintenance_v1',
    'predictive_maintenance',
    '1.0.0',
    CURRENT_TIMESTAMP(),
    'ACTIVE',
    PARSE_JSON('{"accuracy": 0.92, "precision": 0.89, "recall": 0.91, "f1_score": 0.90, "roc_auc": 0.94}'),
    PARSE_JSON('["vehicle_type_numeric", "model_year", "current_mileage", "fuel_efficiency_mpg", "vehicle_age_years", "avg_engine_temperature", "avg_engine_rpm", "avg_fuel_level", "avg_brake_pressure", "engine_temp_volatility", "engine_rpm_volatility", "maintenance_count_12m", "avg_maintenance_cost", "days_since_maintenance", "avg_route_efficiency", "avg_fuel_efficiency", "shipment_count_12m"]'),
    5000,
    0.90
);

-- Create model performance monitoring view
CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.ML_OBJECTS.VW_ML_MODEL_PERFORMANCE AS
SELECT 
    MODEL_NAME,
    MODEL_TYPE,
    MODEL_VERSION,
    STATUS,
    TRAINING_DATE,
    DEPLOYMENT_DATE,
    VALIDATION_SCORE,
    PRODUCTION_SCORE,
    DRIFT_SCORE,
    PREDICTION_COUNT,
    LAST_PREDICTION_DATE,
    -- Extract performance metrics
    PERFORMANCE_METRICS:accuracy::FLOAT as accuracy,
    PERFORMANCE_METRICS:precision::FLOAT as precision,
    PERFORMANCE_METRICS:recall::FLOAT as recall,
    PERFORMANCE_METRICS:f1_score::FLOAT as f1_score,
    PERFORMANCE_METRICS:roc_auc::FLOAT as roc_auc,
    PERFORMANCE_METRICS:mae::FLOAT as mae,
    PERFORMANCE_METRICS:mse::FLOAT as mse,
    PERFORMANCE_METRICS:r2::FLOAT as r2,
    -- Calculate model health score
    CASE 
        WHEN STATUS = 'ACTIVE' AND VALIDATION_SCORE > 0.8 AND DRIFT_SCORE < 0.2 THEN 'HEALTHY'
        WHEN STATUS = 'ACTIVE' AND (VALIDATION_SCORE <= 0.8 OR DRIFT_SCORE >= 0.2) THEN 'DEGRADING'
        WHEN STATUS = 'DEPRECATED' THEN 'DEPRECATED'
        ELSE 'UNKNOWN'
    END as model_health,
    -- Calculate days since last training
    DATEDIFF('day', TRAINING_DATE, CURRENT_DATE()) as days_since_training,
    -- Calculate days since last prediction
    DATEDIFF('day', LAST_PREDICTION_DATE, CURRENT_DATE()) as days_since_prediction
FROM LOGISTICS_DW_PROD.ML_OBJECTS.TBL_ML_MODEL_REGISTRY
ORDER BY TRAINING_DATE DESC;

-- Create model drift detection view
CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.ML_OBJECTS.VW_ML_MODEL_DRIFT_DETECTION AS
SELECT 
    MODEL_NAME,
    MODEL_TYPE,
    DRIFT_SCORE,
    VALIDATION_SCORE,
    PRODUCTION_SCORE,
    -- Calculate drift severity
    CASE 
        WHEN DRIFT_SCORE > 0.3 THEN 'HIGH_DRIFT'
        WHEN DRIFT_SCORE > 0.2 THEN 'MEDIUM_DRIFT'
        WHEN DRIFT_SCORE > 0.1 THEN 'LOW_DRIFT'
        ELSE 'NO_DRIFT'
    END as drift_severity,
    -- Calculate performance degradation
    CASE 
        WHEN PRODUCTION_SCORE IS NOT NULL AND VALIDATION_SCORE IS NOT NULL THEN
            VALIDATION_SCORE - PRODUCTION_SCORE
        ELSE NULL
    END as performance_degradation,
    -- Determine if retraining is needed
    CASE 
        WHEN DRIFT_SCORE > 0.2 OR (PRODUCTION_SCORE IS NOT NULL AND VALIDATION_SCORE IS NOT NULL AND (VALIDATION_SCORE - PRODUCTION_SCORE) > 0.1) THEN 'YES'
        ELSE 'NO'
    END as retraining_needed
FROM LOGISTICS_DW_PROD.ML_OBJECTS.TBL_ML_MODEL_REGISTRY
WHERE STATUS = 'ACTIVE'
ORDER BY DRIFT_SCORE DESC;

-- Create model usage analytics view
CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.ML_OBJECTS.VW_ML_MODEL_USAGE_ANALYTICS AS
SELECT 
    MODEL_NAME,
    MODEL_TYPE,
    PREDICTION_COUNT,
    LAST_PREDICTION_DATE,
    DATEDIFF('day', DEPLOYMENT_DATE, CURRENT_DATE()) as days_in_production,
    -- Calculate predictions per day
    CASE 
        WHEN DATEDIFF('day', DEPLOYMENT_DATE, CURRENT_DATE()) > 0 THEN
            PREDICTION_COUNT / DATEDIFF('day', DEPLOYMENT_DATE, CURRENT_DATE())
        ELSE 0
    END as predictions_per_day,
    -- Calculate usage tier
    CASE 
        WHEN PREDICTION_COUNT > 10000 THEN 'HIGH_USAGE'
        WHEN PREDICTION_COUNT > 1000 THEN 'MEDIUM_USAGE'
        WHEN PREDICTION_COUNT > 100 THEN 'LOW_USAGE'
        ELSE 'MINIMAL_USAGE'
    END as usage_tier
FROM LOGISTICS_DW_PROD.ML_OBJECTS.TBL_ML_MODEL_REGISTRY
WHERE STATUS = 'ACTIVE'
ORDER BY PREDICTION_COUNT DESC;