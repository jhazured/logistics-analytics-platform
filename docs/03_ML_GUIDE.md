# ML/AI Engineer Guide

## 🎯 ML Data Product Benefits

This platform is specifically designed as a **data product for AI engineers** to build machine learning models. It provides:

#### **Feature Engineering**
- **Automated Feature Creation**: 50+ engineered features across customer, vehicle, and operational domains
- **Time-Series Features**: Rolling windows (7d, 30d, 90d) for trend analysis and seasonality
- **Feature Versioning**: Complete lineage tracking and feature store management
- **Real-time Features**: Low-latency feature serving for production ML models

#### **Model Development**
- **Centralized Feature Store**: Single source of truth for all ML features
- **Feature Catalog**: Comprehensive documentation of feature definitions and usage
- **Data Quality**: Built-in validation and monitoring for feature drift
- **Reproducibility**: Version-controlled feature engineering with dbt

#### **Production Deployment**
- **Model Registry**: Complete model lifecycle management with performance tracking
- **Real-time Serving**: Sub-second feature serving for ML inference
- **Monitoring**: Feature drift detection and model performance monitoring
- **Scalability**: Snowflake's auto-scaling infrastructure for ML workloads

## 🚀 Quick Start for ML Engineers

### **1. Access ML Features**

The platform provides a centralized feature store with 50+ engineered features:

```sql
-- Access consolidated feature store
SELECT * FROM tbl_ml_consolidated_feature_store 
WHERE feature_date = CURRENT_DATE();

-- Customer features for segmentation
SELECT 
    customer_id,
    customer_tier_numeric,
    customer_tenure_days,
    customer_on_time_rate_30d,
    customer_reliability_tier
FROM tbl_ml_consolidated_feature_store
WHERE feature_type = 'customer';

-- Vehicle features for predictive maintenance
SELECT 
    vehicle_id,
    vehicle_age_years,
    current_mileage,
    days_since_last_maintenance,
    maintenance_risk_score
FROM tbl_ml_consolidated_feature_store
WHERE feature_type = 'vehicle';
```

### **2. Model Training**

Use the feature store for model training with proper train/validation/test splits:

```python
import snowflake.connector
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='your_user',
    password='your_password',
    account='your_account',
    warehouse='COMPUTE_WH_MEDIUM',
    database='LOGISTICS_DW_PROD',
    schema='ML_FEATURES'
)

# Load training data
query = """
SELECT 
    customer_id,
    customer_tier_numeric,
    customer_tenure_days,
    customer_on_time_rate_30d,
    customer_reliability_tier,
    -- Add more features as needed
FROM tbl_ml_consolidated_feature_store
WHERE feature_date >= DATEADD(day, -90, CURRENT_DATE())
  AND feature_date < CURRENT_DATE()
"""

df = pd.read_sql(query, conn)

# Prepare features and target
X = df.drop(['customer_id', 'target_column'], axis=1)
y = df['target_column']

# Train/validation split
X_train, X_val, y_train, y_val = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Train model
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Evaluate model
val_score = model.score(X_val, y_val)
print(f"Validation Accuracy: {val_score:.3f}")
```

### **3. Model Registry**

Register and track your models in the ML model registry:

```sql
-- Register new model
INSERT INTO tbl_ml_model_registry (
    model_id,
    model_name,
    model_version,
    model_type,
    training_data_source,
    feature_columns,
    model_metrics,
    model_artifact_path,
    created_by,
    created_at
) VALUES (
    'cust_seg_v1.0',
    'Customer Segmentation Model',
    '1.0',
    'classification',
    'tbl_ml_consolidated_feature_store',
    ARRAY_CONSTRUCT('customer_tier_numeric', 'customer_tenure_days', 'customer_on_time_rate_30d'),
    PARSE_JSON('{"accuracy": 0.85, "precision": 0.82, "recall": 0.88}'),
    's3://ml-models/customer_segmentation_v1.0.pkl',
    'ml_engineer@company.com',
    CURRENT_TIMESTAMP()
);

-- Track model performance
INSERT INTO tbl_ml_model_performance (
    model_id,
    evaluation_date,
    accuracy,
    precision,
    recall,
    f1_score,
    data_drift_score
) VALUES (
    'cust_seg_v1.0',
    CURRENT_DATE(),
    0.85,
    0.82,
    0.88,
    0.85,
    0.12
);
```

### **4. Real-time Inference**

Deploy models for real-time inference using the feature serving views:

```sql
-- Real-time customer features for inference
SELECT 
    customer_id,
    customer_tier_numeric,
    customer_tenure_days,
    customer_on_time_rate_30d,
    customer_reliability_tier,
    risk_score
FROM vw_ml_real_time_customer_features
WHERE customer_id = 'CUST_12345';

-- Real-time vehicle features for predictive maintenance
SELECT 
    vehicle_id,
    vehicle_age_years,
    current_mileage,
    days_since_last_maintenance,
    maintenance_risk_score,
    predicted_maintenance_date
FROM vw_ml_real_time_vehicle_features
WHERE vehicle_id = 'VEH_67890';
```

## 📊 ML Feature Catalog

### Customer Features (15 features)
- **Demographics**: `customer_tier_numeric`, `customer_tenure_days`, `industry_code`
- **Behavioral**: `customer_on_time_rate_30d`, `customer_reliability_tier`, `order_frequency_30d`
- **Financial**: `credit_limit_usd`, `average_order_value_30d`, `payment_terms_numeric`
- **Risk**: `risk_score`, `credit_risk_tier`, `payment_delay_risk`

### Vehicle Features (12 features)
- **Specifications**: `vehicle_age_years`, `vehicle_type_numeric`, `capacity_kg`
- **Performance**: `fuel_efficiency_mpg`, `current_mileage`, `maintenance_interval_miles`
- **Status**: `vehicle_status_numeric`, `days_since_last_maintenance`, `maintenance_risk_score`
- **Financial**: `vehicle_profit_margin_30d`, `cost_per_mile_30d`

### Operational Features (18 features)
- **Route**: `route_distance_km`, `route_complexity_score`, `traffic_delay_factor`
- **Weather**: `weather_impact_score`, `temperature_impact`, `precipitation_impact`
- **Performance**: `on_time_delivery_rate_30d`, `fuel_efficiency_30d`, `cost_per_delivery_30d`
- **Predictive**: `delivery_time_prediction`, `fuel_consumption_prediction`, `maintenance_prediction`

### Time-Series Features (8 features)
- **Rolling Averages**: `avg_delivery_time_7d`, `avg_fuel_efficiency_30d`, `avg_cost_per_mile_90d`
- **Trends**: `delivery_time_trend_30d`, `fuel_efficiency_trend_90d`, `cost_trend_90d`
- **Volatility**: `delivery_time_volatility_30d`, `cost_volatility_90d`

## 🎯 Use Cases

### Customer Segmentation
- **Features**: Customer tier, tenure, order frequency, payment behavior
- **Models**: Clustering, classification for customer lifetime value
- **Business Value**: Targeted marketing, pricing optimization

### Predictive Maintenance
- **Features**: Vehicle age, mileage, maintenance history, usage patterns
- **Models**: Time-series forecasting, anomaly detection
- **Business Value**: Reduced downtime, optimized maintenance schedules

### Route Optimization
- **Features**: Distance, traffic patterns, weather conditions, historical performance
- **Models**: Reinforcement learning, optimization algorithms
- **Business Value**: Reduced fuel costs, improved delivery times

### Demand Forecasting
- **Features**: Historical demand, seasonal patterns, customer behavior
- **Models**: Time-series forecasting, regression
- **Business Value**: Inventory optimization, capacity planning

## 🔧 Development Workflow

### 1. Feature Engineering
```bash
# Develop new features in dbt
dbt run --select +tbl_ml_consolidated_feature_store

# Test feature quality
dbt test --select tbl_ml_consolidated_feature_store
```

### 2. Model Development
```python
# Use Jupyter notebooks for experimentation
# Access features via Snowflake connector
# Implement models with scikit-learn, XGBoost, etc.
```

### 3. Model Validation
```sql
-- Validate feature quality
SELECT 
    feature_name,
    null_count,
    null_percentage,
    data_type,
    min_value,
    max_value
FROM vw_ml_feature_monitoring
WHERE feature_date = CURRENT_DATE();
```

### 4. Model Deployment
```sql
-- Register model in registry
-- Deploy to production environment
-- Set up monitoring and alerting
```

## 📈 Performance Optimization

### Feature Store Optimization
- **Clustering**: Tables clustered by `feature_date` and `customer_id`
- **Partitioning**: Time-based partitioning for efficient queries
- **Compression**: Automatic compression for storage optimization

### Real-time Serving
- **Materialized Views**: Pre-computed features for sub-second access
- **Caching**: Intelligent caching for frequently accessed features
- **Auto-scaling**: Dynamic warehouse scaling based on demand

### Monitoring
- **Feature Drift**: Automated detection of feature distribution changes
- **Model Performance**: Real-time model accuracy monitoring
- **Data Quality**: Continuous validation of feature quality

## 🚨 Best Practices

### Feature Engineering
- **Version Control**: All feature engineering in dbt with proper versioning
- **Documentation**: Comprehensive feature documentation and lineage
- **Testing**: Automated testing for feature quality and consistency

### Model Development
- **Reproducibility**: Use versioned features and documented processes
- **Validation**: Proper train/validation/test splits with time-based validation
- **Monitoring**: Continuous monitoring of model performance and drift

### Production Deployment
- **Gradual Rollout**: A/B testing for model deployments
- **Rollback Strategy**: Ability to quickly rollback to previous model versions
- **Alerting**: Proactive alerting for model performance degradation
