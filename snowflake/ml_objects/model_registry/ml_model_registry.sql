-- ML Model Registry
-- Tracks ML models, versions, and performance metrics
-- Essential for ML model lifecycle management

CREATE OR REPLACE TABLE ML_MODELS.MODEL_REGISTRY (
    -- Model identification
    model_id VARCHAR(100) PRIMARY KEY,
    model_name VARCHAR(200) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    model_type VARCHAR(50) NOT NULL, -- 'classification', 'regression', 'clustering', etc.
    
    -- Model metadata
    model_description TEXT,
    model_algorithm VARCHAR(100), -- 'random_forest', 'xgboost', 'neural_network', etc.
    model_framework VARCHAR(50), -- 'scikit-learn', 'tensorflow', 'pytorch', etc.
    
    -- Training information
    training_dataset VARCHAR(200),
    feature_set_version VARCHAR(50),
    training_start_date TIMESTAMP_NTZ,
    training_end_date TIMESTAMP_NTZ,
    training_duration_minutes NUMBER(10),
    
    -- Model performance
    accuracy_score NUMBER(5,4),
    precision_score NUMBER(5,4),
    recall_score NUMBER(5,4),
    f1_score NUMBER(5,4),
    auc_score NUMBER(5,4),
    rmse_score NUMBER(10,4),
    mae_score NUMBER(10,4),
    
    -- Model validation
    validation_dataset VARCHAR(200),
    validation_accuracy NUMBER(5,4),
    validation_precision NUMBER(5,4),
    validation_recall NUMBER(5,4),
    validation_f1 NUMBER(5,4),
    
    -- Model deployment
    deployment_status VARCHAR(20) DEFAULT 'TRAINED', -- 'TRAINED', 'VALIDATED', 'DEPLOYED', 'RETIRED'
    deployment_date TIMESTAMP_NTZ,
    deployment_environment VARCHAR(50), -- 'development', 'staging', 'production'
    
    -- Model artifacts
    model_file_path VARCHAR(500),
    model_file_size_mb NUMBER(10,2),
    feature_importance_json TEXT,
    model_parameters_json TEXT,
    
    -- Model monitoring
    prediction_count NUMBER(15,0) DEFAULT 0,
    last_prediction_date TIMESTAMP_NTZ,
    model_drift_score NUMBER(5,4),
    data_drift_score NUMBER(5,4),
    
    -- Business metrics
    business_impact_score NUMBER(5,2),
    cost_savings_usd NUMBER(15,2),
    revenue_impact_usd NUMBER(15,2),
    
    -- Metadata
    created_by VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    is_active BOOLEAN DEFAULT TRUE
)
CLUSTER BY (model_name, model_version, deployment_status)
COMMENT = 'ML Model Registry for tracking model lifecycle and performance';

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_model_registry_name_version 
ON ML_MODELS.MODEL_REGISTRY (model_name, model_version);

CREATE INDEX IF NOT EXISTS idx_model_registry_deployment 
ON ML_MODELS.MODEL_REGISTRY (deployment_status, deployment_environment);

CREATE INDEX IF NOT EXISTS idx_model_registry_performance 
ON ML_MODELS.MODEL_REGISTRY (accuracy_score, deployment_status);

-- Model performance tracking table
CREATE OR REPLACE TABLE ML_MODELS.MODEL_PERFORMANCE_HISTORY (
    performance_id VARCHAR(100) PRIMARY KEY,
    model_id VARCHAR(100) NOT NULL,
    evaluation_date DATE NOT NULL,
    evaluation_type VARCHAR(50), -- 'training', 'validation', 'production'
    
    -- Performance metrics
    accuracy_score NUMBER(5,4),
    precision_score NUMBER(5,4),
    recall_score NUMBER(5,4),
    f1_score NUMBER(5,4),
    auc_score NUMBER(5,4),
    rmse_score NUMBER(10,4),
    mae_score NUMBER(10,4),
    
    -- Drift metrics
    model_drift_score NUMBER(5,4),
    data_drift_score NUMBER(5,4),
    feature_drift_scores JSON,
    
    -- Business metrics
    prediction_count NUMBER(15,0),
    business_impact_score NUMBER(5,2),
    
    -- Metadata
    evaluation_dataset VARCHAR(200),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    FOREIGN KEY (model_id) REFERENCES ML_MODELS.MODEL_REGISTRY(model_id)
)
CLUSTER BY (model_id, evaluation_date)
COMMENT = 'Historical model performance tracking';

-- Model predictions log
CREATE OR REPLACE TABLE ML_MODELS.MODEL_PREDICTIONS_LOG (
    prediction_id VARCHAR(100) PRIMARY KEY,
    model_id VARCHAR(100) NOT NULL,
    prediction_timestamp TIMESTAMP_NTZ NOT NULL,
    
    -- Input features (key features only for logging)
    customer_id NUMBER,
    vehicle_id VARCHAR(20),
    route_id NUMBER,
    
    -- Prediction results
    prediction_value NUMBER(10,4),
    prediction_probability NUMBER(5,4),
    prediction_confidence NUMBER(5,4),
    
    -- Actual outcome (for model evaluation)
    actual_value NUMBER(10,4),
    prediction_accuracy NUMBER(5,4),
    
    -- Request metadata
    request_id VARCHAR(100),
    user_id VARCHAR(100),
    api_endpoint VARCHAR(200),
    response_time_ms NUMBER(10,0),
    
    -- Metadata
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    FOREIGN KEY (model_id) REFERENCES ML_MODELS.MODEL_REGISTRY(model_id)
)
CLUSTER BY (model_id, prediction_timestamp)
COMMENT = 'Model predictions log for monitoring and evaluation';
