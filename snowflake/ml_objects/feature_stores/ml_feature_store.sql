-- ML Feature Store Table (ML-Optimized)
-- This table is materialized from dbt model: ml_feature_store
-- Optimized for ML training and inference workloads

CREATE OR REPLACE TABLE ML_FEATURES.FEATURE_STORE (
    -- Primary keys for ML workloads
    feature_id VARCHAR(100) PRIMARY KEY,
    customer_id NUMBER NOT NULL,
    vehicle_id VARCHAR(20),
    route_id NUMBER,
    shipment_id NUMBER,
    feature_date DATE NOT NULL,
    
    -- Customer features (ML-optimized)
    customer_tier_numeric NUMBER(1),
    customer_tenure_days NUMBER(6),
    credit_limit_usd NUMBER(15,2),
    customer_tier VARCHAR(20),
    industry_code VARCHAR(50),
    
    -- Vehicle features (ML-optimized)
    vehicle_age_years NUMBER(3),
    vehicle_type_numeric NUMBER(1),
    capacity_kg NUMBER(10,2),
    fuel_efficiency_mpg NUMBER(10,3),
    current_mileage NUMBER(12,0),
    vehicle_status VARCHAR(20),
    
    -- Route features (ML-optimized)
    distance_km NUMBER(8,1),
    estimated_travel_time_hours NUMBER(6,2),
    route_type_numeric NUMBER(1),
    route_complexity_score NUMBER(3),
    
    -- Shipment features (ML-optimized)
    actual_delivery_time_hours NUMBER(6,2),
    estimated_delivery_time_hours NUMBER(6,2),
    delivery_time_variance_hours NUMBER(6,2),
    is_delayed BOOLEAN,
    on_time_delivery_flag BOOLEAN,
    revenue NUMBER(10,2),
    total_cost NUMBER(10,2),
    profit_margin_pct NUMBER(5,2),
    route_efficiency_score NUMBER(5,2),
    carbon_emissions_kg NUMBER(8,2),
    weather_delay_minutes NUMBER(6,1),
    traffic_delay_minutes NUMBER(6,1),
    
    -- Rolling averages (ML features)
    customer_on_time_rate_30d NUMBER(5,4),
    route_efficiency_30d NUMBER(5,2),
    vehicle_profit_margin_30d NUMBER(5,2),
    
    -- Feature engineering
    customer_reliability_tier VARCHAR(20),
    risk_score NUMBER(5,4),
    feature_hash VARCHAR(64),
    
    -- Metadata
    feature_created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    feature_version VARCHAR(20) DEFAULT 'v1.0',
    model_training_date DATE,
    is_training_data BOOLEAN DEFAULT TRUE,
    is_serving_data BOOLEAN DEFAULT TRUE
)
CLUSTER BY (customer_id, feature_date, feature_version)
COMMENT = 'ML Feature Store optimized for training and inference workloads';

-- Create indexes for ML query patterns
CREATE INDEX IF NOT EXISTS idx_ml_feature_store_customer_date 
ON ML_FEATURES.FEATURE_STORE (customer_id, feature_date);

CREATE INDEX IF NOT EXISTS idx_ml_feature_store_vehicle_date 
ON ML_FEATURES.FEATURE_STORE (vehicle_id, feature_date);

CREATE INDEX IF NOT EXISTS idx_ml_feature_store_training 
ON ML_FEATURES.FEATURE_STORE (is_training_data, feature_date);

CREATE INDEX IF NOT EXISTS idx_ml_feature_store_serving 
ON ML_FEATURES.FEATURE_STORE (is_serving_data, feature_date);
