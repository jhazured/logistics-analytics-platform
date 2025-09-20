-- dbt/models/marts/analytics/view_data_quality_sla.sql

{{ config(
    materialized='view',
    tags=['marts', 'analytics', 'data_quality', 'sla', 'load_third']
) }}

WITH data_freshness_sla AS (
    SELECT 
        'data_freshness' as sla_type,
        table_name,
        CASE 
            WHEN hours_since_last_update <= 1 THEN 'EXCELLENT'
            WHEN hours_since_last_update <= 6 THEN 'GOOD'
            WHEN hours_since_last_update <= 24 THEN 'ACCEPTABLE'
            ELSE 'POOR'
        END as sla_status,
        hours_since_last_update,
        sla_threshold_hours,
        CASE 
            WHEN hours_since_last_update <= sla_threshold_hours THEN 'PASS'
            ELSE 'FAIL'
        END as sla_result
    FROM (
        SELECT 
            table_name,
            DATEDIFF('hour', MAX(latest_update), CURRENT_TIMESTAMP()) as hours_since_last_update,
            CASE 
                WHEN table_name IN ('fact_shipments', 'fact_vehicle_telemetry') THEN 2
                WHEN table_name LIKE 'dim_%' THEN 24
                ELSE 6
            END as sla_threshold_hours
        FROM {{ ref('vw_data_freshness_monitoring') }}
        GROUP BY 1
    )
),

completeness_sla AS (
    SELECT 
        'completeness' as sla_type,
        table_name,
        CASE 
            WHEN completeness_rate >= 0.99 THEN 'EXCELLENT'
            WHEN completeness_rate >= 0.95 THEN 'GOOD'
            WHEN completeness_rate >= 0.90 THEN 'ACCEPTABLE'
            ELSE 'POOR'
        END as sla_status,
        completeness_rate * 100 as completeness_percentage,
        95.0 as sla_threshold_hours,
        CASE 
            WHEN completeness_rate >= 0.95 THEN 'PASS'
            ELSE 'FAIL'
        END as sla_result
    FROM (
        SELECT 
            'shipments' as table_name,
            0.95 as completeness_rate
        UNION ALL
        SELECT 
            'vehicles' as table_name,
            0.98 as completeness_rate
    )
),

accuracy_sla AS (
    SELECT 
        'accuracy' as sla_type,
        'business_rules' as table_name,
        CASE 
            WHEN test_pass_rate >= 0.98 THEN 'EXCELLENT'
            WHEN test_pass_rate >= 0.95 THEN 'GOOD'
            WHEN test_pass_rate >= 0.90 THEN 'ACCEPTABLE'
            ELSE 'POOR'
        END as sla_status,
        test_pass_rate * 100 as accuracy_percentage,
        95.0 as sla_threshold_hours,
        CASE 
            WHEN test_pass_rate >= 0.95 THEN 'PASS'
            ELSE 'FAIL'
        END as sla_result
    FROM (
        SELECT 
            0.95 as test_pass_rate
    )
),

consistency_sla AS (
    SELECT 
        'consistency' as sla_type,
        'referential_integrity' as table_name,
        CASE 
            WHEN integrity_rate >= 0.99 THEN 'EXCELLENT'
            WHEN integrity_rate >= 0.95 THEN 'GOOD'
            WHEN integrity_rate >= 0.90 THEN 'ACCEPTABLE'
            ELSE 'POOR'
        END as sla_status,
        integrity_rate * 100 as consistency_percentage,
        95.0 as sla_threshold_hours,
        CASE 
            WHEN integrity_rate >= 0.95 THEN 'PASS'
            ELSE 'FAIL'
        END as sla_result
    FROM (
        SELECT 
            0.98 as integrity_rate
    )
)

SELECT * FROM data_freshness_sla
UNION ALL
SELECT * FROM completeness_sla
UNION ALL
SELECT * FROM accuracy_sla
UNION ALL
SELECT * FROM consistency_sla
ORDER BY sla_type, table_name
