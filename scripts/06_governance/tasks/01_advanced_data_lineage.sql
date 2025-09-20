-- Advanced Data Lineage with Business Impact Analysis
-- This script creates comprehensive data lineage tracking with business impact scoring

-- Create advanced lineage tracking table
CREATE OR REPLACE TABLE LOGISTICS_DW_PROD.GOVERNANCE.ADVANCED_DATA_LINEAGE (
    LINEAGE_ID VARCHAR(50) DEFAULT UUID_STRING(),
    SOURCE_TABLE VARCHAR(255) NOT NULL,
    TARGET_TABLE VARCHAR(255) NOT NULL,
    TRANSFORMATION_TYPE VARCHAR(100) NOT NULL,
    BUSINESS_IMPACT_SCORE FLOAT NOT NULL,
    DATA_QUALITY_SCORE FLOAT NOT NULL,
    PERFORMANCE_IMPACT FLOAT NOT NULL,
    COST_IMPACT FLOAT NOT NULL,
    CRITICALITY_LEVEL VARCHAR(20) NOT NULL,
    BUSINESS_OWNER VARCHAR(255),
    TECHNICAL_OWNER VARCHAR(255),
    LAST_UPDATED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LINEAGE_METADATA VARIANT
);

-- Create business impact scoring function
CREATE OR REPLACE FUNCTION calculate_business_impact_score(
    table_name VARCHAR,
    usage_frequency INT,
    downstream_dependencies INT,
    business_criticality VARCHAR
)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    CASE 
        WHEN business_criticality = 'CRITICAL' THEN 1.0
        WHEN business_criticality = 'HIGH' THEN 0.8
        WHEN business_criticality = 'MEDIUM' THEN 0.6
        WHEN business_criticality = 'LOW' THEN 0.4
        ELSE 0.2
    END * 
    (LEAST(usage_frequency / 100.0, 1.0) * 0.4 + 
     LEAST(downstream_dependencies / 10.0, 1.0) * 0.6)
$$;

-- Create data quality scoring function
CREATE OR REPLACE FUNCTION calculate_data_quality_score(table_name VARCHAR)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
DECLARE
    completeness_score FLOAT;
    accuracy_score FLOAT;
    consistency_score FLOAT;
    timeliness_score FLOAT;
    overall_score FLOAT;
BEGIN
    -- Calculate completeness
    EXECUTE IMMEDIATE 'SELECT (COUNT(*) - COUNT(NULL)) / COUNT(*) FROM ' || table_name INTO completeness_score;
    
    -- Calculate accuracy (business rule validation)
    SELECT AVG(CASE WHEN sla_result = 'PASS' THEN 1.0 ELSE 0.0 END) INTO accuracy_score
    FROM LOGISTICS_DW_PROD.MONITORING.VW_DATA_QUALITY_SLA 
    WHERE table_name = table_name;
    
    -- Calculate consistency (referential integrity)
    SELECT AVG(CASE WHEN test_result = 'PASS' THEN 1.0 ELSE 0.0 END) INTO consistency_score
    FROM LOGISTICS_DW_PROD.MONITORING.TEST_RESULTS 
    WHERE test_name LIKE '%referential%' AND table_name = table_name;
    
    -- Calculate timeliness
    SELECT CASE 
        WHEN minutes_since_sync <= 60 THEN 1.0
        WHEN minutes_since_sync <= 360 THEN 0.8
        WHEN minutes_since_sync <= 720 THEN 0.6
        ELSE 0.0
    END INTO timeliness_score
    FROM LOGISTICS_DW_PROD.MONITORING.VW_DATA_FRESHNESS_MONITORING 
    WHERE table_name = table_name;
    
    -- Calculate overall score
    overall_score := (completeness_score * 0.3 + accuracy_score * 0.3 + 
                     consistency_score * 0.2 + timeliness_score * 0.2);
    
    RETURN overall_score;
END;
$$;

-- Create performance impact scoring function
CREATE OR REPLACE FUNCTION calculate_performance_impact(
    table_name VARCHAR,
    query_frequency INT,
    avg_query_time FLOAT,
    data_volume_gb FLOAT
)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    -- Normalize and weight performance factors
    (LEAST(query_frequency / 1000.0, 1.0) * 0.4 + 
     LEAST(avg_query_time / 60.0, 1.0) * 0.3 + 
     LEAST(data_volume_gb / 100.0, 1.0) * 0.3)
$$;

-- Create cost impact scoring function
CREATE OR REPLACE FUNCTION calculate_cost_impact(
    table_name VARCHAR,
    storage_cost_usd FLOAT,
    compute_cost_usd FLOAT,
    maintenance_cost_usd FLOAT
)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    -- Normalize cost impact (higher cost = higher impact)
    LEAST((storage_cost_usd + compute_cost_usd + maintenance_cost_usd) / 1000.0, 1.0)
$$;

-- Populate advanced lineage with business impact analysis
INSERT INTO LOGISTICS_DW_PROD.GOVERNANCE.ADVANCED_DATA_LINEAGE (
    SOURCE_TABLE, TARGET_TABLE, TRANSFORMATION_TYPE, 
    BUSINESS_IMPACT_SCORE, DATA_QUALITY_SCORE, PERFORMANCE_IMPACT, COST_IMPACT,
    CRITICALITY_LEVEL, BUSINESS_OWNER, TECHNICAL_OWNER, LINEAGE_METADATA
)
WITH lineage_analysis AS (
    SELECT 
        'raw_logistics.shipments' as source_table,
        'tbl_stg_shipments' as target_table,
        'data_cleaning' as transformation_type,
        calculate_business_impact_score('tbl_stg_shipments', 500, 15, 'CRITICAL') as business_impact,
        calculate_data_quality_score('tbl_stg_shipments') as data_quality,
        calculate_performance_impact('tbl_stg_shipments', 200, 30, 50) as performance_impact,
        calculate_cost_impact('tbl_stg_shipments', 100, 200, 50) as cost_impact,
        'Operations Team' as business_owner,
        'Data Engineering Team' as technical_owner,
        OBJECT_CONSTRUCT(
            'transformation_rules', ARRAY_CONSTRUCT('data_type_conversion', 'null_handling', 'validation'),
            'business_rules', ARRAY_CONSTRUCT('shipment_status_validation', 'date_range_validation'),
            'dependencies', ARRAY_CONSTRUCT('raw_logistics.shipments'),
            'sla_requirements', OBJECT_CONSTRUCT('freshness_hours', 2, 'quality_threshold', 0.95)
        ) as lineage_metadata
    
    UNION ALL
    
    SELECT 
        'tbl_stg_shipments' as source_table,
        'tbl_fact_shipments' as target_table,
        'dimensional_modeling' as transformation_type,
        calculate_business_impact_score('tbl_fact_shipments', 800, 25, 'CRITICAL') as business_impact,
        calculate_data_quality_score('tbl_fact_shipments') as data_quality,
        calculate_performance_impact('tbl_fact_shipments', 500, 45, 100) as performance_impact,
        calculate_cost_impact('tbl_fact_shipments', 200, 400, 100) as cost_impact,
        'Operations Team' as business_owner,
        'Data Engineering Team' as technical_owner,
        OBJECT_CONSTRUCT(
            'transformation_rules', ARRAY_CONSTRUCT('surrogate_key_generation', 'calculated_fields', 'dimension_joins'),
            'business_rules', ARRAY_CONSTRUCT('profit_margin_calculation', 'route_efficiency_scoring'),
            'dependencies', ARRAY_CONSTRUCT('tbl_stg_shipments', 'tbl_dim_customer', 'tbl_dim_vehicle', 'tbl_dim_route'),
            'sla_requirements', OBJECT_CONSTRUCT('freshness_hours', 1, 'quality_threshold', 0.98)
        ) as lineage_metadata
    
    UNION ALL
    
    SELECT 
        'tbl_fact_shipments' as source_table,
        'tbl_ml_consolidated_feature_store' as target_table,
        'feature_engineering' as transformation_type,
        calculate_business_impact_score('tbl_ml_consolidated_feature_store', 300, 10, 'HIGH') as business_impact,
        calculate_data_quality_score('tbl_ml_consolidated_feature_store') as data_quality,
        calculate_performance_impact('tbl_ml_consolidated_feature_store', 100, 60, 75) as performance_impact,
        calculate_cost_impact('tbl_ml_consolidated_feature_store', 150, 300, 75) as cost_impact,
        'ML Engineering Team' as business_owner,
        'Data Engineering Team' as technical_owner,
        OBJECT_CONSTRUCT(
            'transformation_rules', ARRAY_CONSTRUCT('feature_aggregation', 'rolling_windows', 'ml_optimization'),
            'business_rules', ARRAY_CONSTRUCT('customer_segmentation', 'predictive_maintenance', 'route_optimization'),
            'dependencies', ARRAY_CONSTRUCT('tbl_fact_shipments', 'tbl_dim_customer', 'tbl_dim_vehicle'),
            'sla_requirements', OBJECT_CONSTRUCT('freshness_hours', 4, 'quality_threshold', 0.90)
        ) as lineage_metadata
    
    UNION ALL
    
    SELECT 
        'tbl_ml_consolidated_feature_store' as source_table,
        'vw_ml_real_time_customer_features' as target_table,
        'real_time_serving' as transformation_type,
        calculate_business_impact_score('vw_ml_real_time_customer_features', 200, 5, 'MEDIUM') as business_impact,
        calculate_data_quality_score('vw_ml_real_time_customer_features') as data_quality,
        calculate_performance_impact('vw_ml_real_time_customer_features', 50, 5, 25) as performance_impact,
        calculate_cost_impact('vw_ml_real_time_customer_features', 50, 100, 25) as cost_impact,
        'ML Engineering Team' as business_owner,
        'Data Engineering Team' as technical_owner,
        OBJECT_CONSTRUCT(
            'transformation_rules', ARRAY_CONSTRUCT('real_time_filtering', 'latency_optimization'),
            'business_rules', ARRAY_CONSTRUCT('customer_feature_serving', 'ml_inference_optimization'),
            'dependencies', ARRAY_CONSTRUCT('tbl_ml_consolidated_feature_store'),
            'sla_requirements', OBJECT_CONSTRUCT('freshness_minutes', 5, 'quality_threshold', 0.85)
        ) as lineage_metadata
)
SELECT 
    source_table,
    target_table,
    transformation_type,
    business_impact,
    data_quality,
    performance_impact,
    cost_impact,
    CASE 
        WHEN business_impact >= 0.8 THEN 'CRITICAL'
        WHEN business_impact >= 0.6 THEN 'HIGH'
        WHEN business_impact >= 0.4 THEN 'MEDIUM'
        ELSE 'LOW'
    END as criticality_level,
    business_owner,
    technical_owner,
    lineage_metadata
FROM lineage_analysis;

-- Create comprehensive lineage view with business impact
CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.GOVERNANCE.VW_ADVANCED_DATA_LINEAGE AS
WITH lineage_with_impact AS (
    SELECT 
        *,
        -- Calculate overall impact score
        (business_impact_score * 0.4 + data_quality_score * 0.3 + 
         performance_impact * 0.2 + cost_impact * 0.1) as overall_impact_score,
        
        -- Calculate risk score
        CASE 
            WHEN data_quality_score < 0.8 THEN 'HIGH_RISK'
            WHEN performance_impact > 0.7 THEN 'MEDIUM_RISK'
            WHEN cost_impact > 0.8 THEN 'MEDIUM_RISK'
            ELSE 'LOW_RISK'
        END as risk_level,
        
        -- Calculate business priority
        CASE 
            WHEN business_impact_score >= 0.8 AND data_quality_score >= 0.9 THEN 'P0'
            WHEN business_impact_score >= 0.6 AND data_quality_score >= 0.8 THEN 'P1'
            WHEN business_impact_score >= 0.4 AND data_quality_score >= 0.7 THEN 'P2'
            ELSE 'P3'
        END as business_priority
    FROM LOGISTICS_DW_PROD.GOVERNANCE.ADVANCED_DATA_LINEAGE
),
lineage_dependencies AS (
    SELECT 
        source_table,
        COUNT(*) as downstream_count,
        ARRAY_AGG(target_table) as downstream_tables
    FROM LOGISTICS_DW_PROD.GOVERNANCE.ADVANCED_DATA_LINEAGE
    GROUP BY source_table
)
SELECT 
    l.*,
    ld.downstream_count,
    ld.downstream_tables,
    -- Add business context
    CASE 
        WHEN l.transformation_type = 'data_cleaning' THEN 'Data Quality & Validation'
        WHEN l.transformation_type = 'dimensional_modeling' THEN 'Business Intelligence & Analytics'
        WHEN l.transformation_type = 'feature_engineering' THEN 'Machine Learning & AI'
        WHEN l.transformation_type = 'real_time_serving' THEN 'Real-time Operations'
        ELSE 'Other'
    END as business_context,
    
    -- Add technical context
    CASE 
        WHEN l.performance_impact > 0.7 THEN 'Performance Critical'
        WHEN l.cost_impact > 0.7 THEN 'Cost Critical'
        WHEN l.data_quality_score < 0.8 THEN 'Quality Critical'
        ELSE 'Standard'
    END as technical_context
FROM lineage_with_impact l
LEFT JOIN lineage_dependencies ld ON l.source_table = ld.source_table
ORDER BY l.overall_impact_score DESC, l.business_impact_score DESC;

-- Create data lineage impact monitoring
CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.GOVERNANCE.VW_LINEAGE_IMPACT_MONITORING AS
SELECT 
    DATE_TRUNC('day', last_updated) as monitoring_date,
    criticality_level,
    COUNT(*) as table_count,
    AVG(business_impact_score) as avg_business_impact,
    AVG(data_quality_score) as avg_data_quality,
    AVG(performance_impact) as avg_performance_impact,
    AVG(cost_impact) as avg_cost_impact,
    AVG(overall_impact_score) as avg_overall_impact,
    SUM(CASE WHEN risk_level = 'HIGH_RISK' THEN 1 ELSE 0 END) as high_risk_count,
    SUM(CASE WHEN business_priority = 'P0' THEN 1 ELSE 0 END) as p0_priority_count
FROM LOGISTICS_DW_PROD.GOVERNANCE.VW_ADVANCED_DATA_LINEAGE
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- Create automated lineage impact alerts
CREATE OR REPLACE PROCEDURE LOGISTICS_DW_PROD.GOVERNANCE.SP_check_lineage_impact_alerts()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    alert_count INT;
    alert_message STRING;
BEGIN
    -- Check for high-risk lineage issues
    SELECT COUNT(*) INTO alert_count
    FROM LOGISTICS_DW_PROD.GOVERNANCE.VW_ADVANCED_DATA_LINEAGE
    WHERE risk_level = 'HIGH_RISK' 
    AND data_quality_score < 0.8;
    
    IF alert_count > 0 THEN
        alert_message := 'ALERT: ' || alert_count || ' high-risk data lineage issues detected. Data quality below threshold.';
        
        -- Insert alert
        INSERT INTO LOGISTICS_DW_PROD.MONITORING.ALERT_HISTORY (
            alert_type, alert_name, severity, alert_message, alert_data
        ) VALUES (
            'data_lineage', 'High Risk Lineage Alert', 'HIGH', 
            alert_message, 
            OBJECT_CONSTRUCT('high_risk_count', alert_count)
        );
    END IF;
    
    RETURN alert_message;
END;
$$;

-- Create lineage impact dashboard
CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.GOVERNANCE.VW_LINEAGE_IMPACT_DASHBOARD AS
SELECT 
    'Data Lineage Overview' as metric_category,
    COUNT(*) as total_tables,
    COUNT(CASE WHEN criticality_level = 'CRITICAL' THEN 1 END) as critical_tables,
    COUNT(CASE WHEN criticality_level = 'HIGH' THEN 1 END) as high_priority_tables,
    AVG(overall_impact_score) as avg_impact_score,
    COUNT(CASE WHEN risk_level = 'HIGH_RISK' THEN 1 END) as high_risk_tables,
    COUNT(CASE WHEN business_priority = 'P0' THEN 1 END) as p0_priority_tables
FROM LOGISTICS_DW_PROD.GOVERNANCE.VW_ADVANCED_DATA_LINEAGE

UNION ALL

SELECT 
    'Business Impact Analysis' as metric_category,
    COUNT(*) as total_tables,
    COUNT(CASE WHEN business_impact_score >= 0.8 THEN 1 END) as critical_tables,
    COUNT(CASE WHEN business_impact_score >= 0.6 THEN 1 END) as high_priority_tables,
    AVG(business_impact_score) as avg_impact_score,
    COUNT(CASE WHEN data_quality_score < 0.8 THEN 1 END) as high_risk_tables,
    COUNT(CASE WHEN performance_impact > 0.7 THEN 1 END) as p0_priority_tables
FROM LOGISTICS_DW_PROD.GOVERNANCE.VW_ADVANCED_DATA_LINEAGE

UNION ALL

SELECT 
    'Technical Performance' as metric_category,
    COUNT(*) as total_tables,
    COUNT(CASE WHEN performance_impact >= 0.8 THEN 1 END) as critical_tables,
    COUNT(CASE WHEN performance_impact >= 0.6 THEN 1 END) as high_priority_tables,
    AVG(performance_impact) as avg_impact_score,
    COUNT(CASE WHEN cost_impact > 0.8 THEN 1 END) as high_risk_tables,
    COUNT(CASE WHEN data_quality_score < 0.7 THEN 1 END) as p0_priority_tables
FROM LOGISTICS_DW_PROD.GOVERNANCE.VW_ADVANCED_DATA_LINEAGE;
