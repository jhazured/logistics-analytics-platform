-- ML Feature Monitoring Dashboard
-- Monitors feature quality, drift, and performance for ML models

CREATE OR REPLACE VIEW ML_MONITORING.FEATURE_MONITORING AS
WITH feature_quality_metrics AS (
    SELECT 
        feature_date,
        COUNT(*) as total_features,
        COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as missing_customer_ids,
        COUNT(CASE WHEN vehicle_id IS NULL THEN 1 END) as missing_vehicle_ids,
        COUNT(CASE WHEN feature_date IS NULL THEN 1 END) as missing_dates,
        COUNT(CASE WHEN risk_score IS NULL THEN 1 END) as missing_risk_scores,
        AVG(risk_score) as avg_risk_score,
        STDDEV(risk_score) as risk_score_stddev,
        MIN(risk_score) as min_risk_score,
        MAX(risk_score) as max_risk_score
    FROM ML_FEATURES.FEATURE_STORE
    WHERE feature_date >= CURRENT_DATE() - 30
    GROUP BY feature_date
),
feature_drift_metrics AS (
    SELECT 
        feature_date,
        -- Compare with previous day
        LAG(AVG(risk_score), 1) OVER (ORDER BY feature_date) as prev_day_avg_risk,
        LAG(AVG(customer_on_time_rate_30d), 1) OVER (ORDER BY feature_date) as prev_day_avg_on_time,
        LAG(AVG(route_efficiency_30d), 1) OVER (ORDER BY feature_date) as prev_day_avg_efficiency,
        
        -- Current day metrics
        AVG(risk_score) as current_avg_risk,
        AVG(customer_on_time_rate_30d) as current_avg_on_time,
        AVG(route_efficiency_30d) as current_avg_efficiency,
        
        -- Drift calculations
        ABS(AVG(risk_score) - LAG(AVG(risk_score), 1) OVER (ORDER BY feature_date)) as risk_score_drift,
        ABS(AVG(customer_on_time_rate_30d) - LAG(AVG(customer_on_time_rate_30d), 1) OVER (ORDER BY feature_date)) as on_time_drift,
        ABS(AVG(route_efficiency_30d) - LAG(AVG(route_efficiency_30d), 1) OVER (ORDER BY feature_date)) as efficiency_drift
        
    FROM ML_FEATURES.FEATURE_STORE
    WHERE feature_date >= CURRENT_DATE() - 30
    GROUP BY feature_date
),
feature_volume_metrics AS (
    SELECT 
        feature_date,
        COUNT(DISTINCT customer_id) as unique_customers,
        COUNT(DISTINCT vehicle_id) as unique_vehicles,
        COUNT(DISTINCT route_id) as unique_routes,
        COUNT(*) as total_feature_records,
        COUNT(CASE WHEN is_training_data THEN 1 END) as training_records,
        COUNT(CASE WHEN is_serving_data THEN 1 END) as serving_records
    FROM ML_FEATURES.FEATURE_STORE
    WHERE feature_date >= CURRENT_DATE() - 30
    GROUP BY feature_date
)
SELECT 
    qm.feature_date,
    
    -- Quality metrics
    qm.total_features,
    qm.missing_customer_ids,
    qm.missing_vehicle_ids,
    qm.missing_dates,
    qm.missing_risk_scores,
    qm.avg_risk_score,
    qm.risk_score_stddev,
    qm.min_risk_score,
    qm.max_risk_score,
    
    -- Data quality score (0-100)
    CASE 
        WHEN qm.total_features = 0 THEN 0
        ELSE ROUND(
            (1 - (qm.missing_customer_ids + qm.missing_vehicle_ids + qm.missing_dates + qm.missing_risk_scores) / (qm.total_features * 4.0)) * 100, 2
        )
    END as data_quality_score,
    
    -- Drift metrics
    dm.risk_score_drift,
    dm.on_time_drift,
    dm.efficiency_drift,
    
    -- Overall drift score (0-100, higher is worse)
    ROUND(
        (COALESCE(dm.risk_score_drift, 0) + COALESCE(dm.on_time_drift, 0) + COALESCE(dm.efficiency_drift, 0)) * 100, 2
    ) as overall_drift_score,
    
    -- Volume metrics
    vm.unique_customers,
    vm.unique_vehicles,
    vm.unique_routes,
    vm.total_feature_records,
    vm.training_records,
    vm.serving_records,
    
    -- Alert indicators
    CASE 
        WHEN qm.total_features = 0 THEN 'NO_DATA'
        WHEN (qm.missing_customer_ids + qm.missing_vehicle_ids + qm.missing_dates + qm.missing_risk_scores) / qm.total_features > 0.1 THEN 'HIGH_MISSING_DATA'
        WHEN (COALESCE(dm.risk_score_drift, 0) + COALESCE(dm.on_time_drift, 0) + COALESCE(dm.efficiency_drift, 0)) > 0.1 THEN 'HIGH_DRIFT'
        WHEN vm.total_feature_records < LAG(vm.total_feature_records, 1) OVER (ORDER BY qm.feature_date) * 0.8 THEN 'LOW_VOLUME'
        ELSE 'HEALTHY'
    END as alert_status,
    
    CURRENT_TIMESTAMP() as monitoring_timestamp

FROM feature_quality_metrics qm
LEFT JOIN feature_drift_metrics dm ON qm.feature_date = dm.feature_date
LEFT JOIN feature_volume_metrics vm ON qm.feature_date = vm.feature_date
ORDER BY qm.feature_date DESC
COMMENT = 'ML Feature monitoring dashboard for data quality and drift detection';
