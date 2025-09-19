-- 17. dbt Run Results
-- File: models/analytics/monitoring/view_dbt_run_results.sql
{{ config(
    materialized='view',
    tags=['monitoring', 'dbt', 'pipeline']
) }}

-- Note: This would typically pull from dbt Cloud API or run_results.json
-- For demo purposes, we'll simulate pipeline health based on model patterns

WITH model_execution_simulation AS (
    SELECT 
        'view_haul_segmentation' AS model_name,
        'analytics.ml_features' AS schema_name,
        'table' AS materialization,
        'success' AS status,
        15.3 AS execution_time_seconds,
        125000 AS rows_affected,
        CURRENT_TIMESTAMP() - INTERVAL '45 minutes' AS completed_at,
        NULL AS error_message
        
    UNION ALL
    
    SELECT 
        'view_customer_behavior_segments' AS model_name,
        'analytics.ml_features' AS schema_name,
        'table' AS materialization,
        'success' AS status,
        23.7 AS execution_time_seconds,
        1000 AS rows_affected,
        CURRENT_TIMESTAMP() - INTERVAL '44 minutes' AS completed_at,
        NULL AS error_message
        
    UNION ALL
    
    SELECT 
        'view_route_optimization_features' AS model_name,
        'analytics.ml_features' AS schema_name,
        'table' AS materialization,
        'success' AS status,
        45.2 AS execution_time_seconds,
        45000 AS rows_affected,
        CURRENT_TIMESTAMP() - INTERVAL '42 minutes' AS completed_at,
        NULL AS error_message
        
    UNION ALL
    
    SELECT 
        'view_operational_performance_rolling' AS model_name,
        'analytics.rolling_analytics' AS schema_name,
        'table' AS materialization,
        'success' AS status,
        67.8 AS execution_time_seconds,
        89000 AS rows_affected,
        CURRENT_TIMESTAMP() - INTERVAL '41 minutes' AS completed_at,
        NULL AS error_message
        
    UNION ALL
    
    SELECT 
        'view_performance_dashboard' AS model_name,
        'analytics.business_intelligence' AS schema_name,
        'view' AS materialization,
        'success' AS status,
        8.9 AS execution_time_seconds,
        NULL AS rows_affected,
        CURRENT_TIMESTAMP() - INTERVAL '40 minutes' AS completed_at,
        NULL AS error_message
),

model_performance_metrics AS (
    SELECT 
        *,
        -- Performance classification
        CASE 
            WHEN execution_time_seconds < 10 THEN 'fast'
            WHEN execution_time_seconds < 30 THEN 'moderate'
            WHEN execution_time_seconds < 60 THEN 'slow'
            ELSE 'very_slow'
        END AS performance_category,
        
        -- Resource utilization estimate
        CASE 
            WHEN materialization = 'table' AND execution_time_seconds > 60 THEN 'high_compute'
            WHEN materialization = 'table' AND execution_time_seconds > 30 THEN 'medium_compute'
            WHEN materialization = 'view' THEN 'low_compute'
            ELSE 'standard_compute'
        END AS resource_usage,
        
        -- Pipeline health contribution
        CASE 
            WHEN status = 'success' AND execution_time_seconds < 60 THEN 100
            WHEN status = 'success' AND execution_time_seconds < 120 THEN 85
            WHEN status = 'success' THEN 70
            WHEN status = 'warning' THEN 40
            ELSE 0
        END AS health_score

    FROM model_execution_simulation
)

SELECT 
    model_name,
    schema_name,
    materialization,
    status,
    execution_time_seconds,
    rows_affected,
    completed_at,
    error_message,
    performance_category,
    resource_usage,
    health_score,
    
    -- Optimization recommendations
    CASE 
        WHEN performance_category = 'very_slow' AND materialization = 'table' 
        THEN 'Consider incremental strategy or clustering'
        WHEN performance_category = 'slow' AND rows_affected > 100000 
        THEN 'Review query optimization and indexing'
        WHEN resource_usage = 'high_compute' 
        THEN 'Consider warehouse sizing or query optimization'
        ELSE 'Performance acceptable'
    END AS optimization_recommendation,
    
    -- Documentation and testing coverage
    CASE 
        WHEN model_name LIKE 'view_%' THEN TRUE
        ELSE FALSE
    END AS has_documentation,
    
    CASE 
        WHEN schema_name LIKE '%ml_features%' OR schema_name LIKE '%analytics%' THEN TRUE
        ELSE FALSE
    END AS has_tests,
    
    -- Pipeline dependency impact
    CASE 
        WHEN schema_name LIKE '%ml_features%' THEN 'high_impact'
        WHEN schema_name LIKE '%business_intelligence%' THEN 'medium_impact'
        ELSE 'low_impact'
    END AS downstream_impact,
    
    CURRENT_TIMESTAMP() AS analysis_timestamp

FROM model_performance_metrics