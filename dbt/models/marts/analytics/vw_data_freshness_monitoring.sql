-- =====================================================
-- Data Quality & Monitoring Views
-- =====================================================

-- Data Freshness Monitoring
{{ config(
    materialized='view',
    tags=['marts', 'analytics', 'monitoring', 'quality', 'freshness', 'load_third']
) }}

WITH source_freshness AS (
    SELECT 
        'fact_shipments' AS table_name,
        'shipments' AS data_source,
        MAX(updated_at) AS latest_update,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN updated_at >= CURRENT_TIMESTAMP() - INTERVAL '{{ var("data_freshness_hours") }} hours' THEN 1 END) AS recent_updates,
        DATEDIFF(minute, MAX(updated_at), CURRENT_TIMESTAMP()) AS minutes_since_update
    FROM {{ ref('tbl_fact_shipments') }}
    
    UNION ALL
    
    SELECT 
        'fact_vehicle_telemetry' AS table_name,
        'vehicle_telematics' AS data_source,
        MAX(created_at) AS latest_update,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN created_at >= CURRENT_TIMESTAMP() - INTERVAL '{{ var("critical_freshness_hours") }} hours' THEN 1 END) AS recent_updates,
        DATEDIFF(minute, MAX(created_at), CURRENT_TIMESTAMP()) AS minutes_since_update
    FROM {{ ref('tbl_fact_vehicle_telemetry') }}
    
    UNION ALL
    
    SELECT 
        'dim_weather' AS table_name,
        'weather_api' AS data_source,
        MAX(created_at) AS latest_update,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN created_at >= CURRENT_TIMESTAMP() - INTERVAL '{{ var("data_freshness_hours") }} hours' THEN 1 END) AS recent_updates,
        DATEDIFF(minute, MAX(created_at), CURRENT_TIMESTAMP()) AS minutes_since_update
    FROM {{ ref('tbl_dim_weather') }}
    
    UNION ALL
    
    SELECT 
        'dim_customer' AS table_name,
        'customer_system' AS data_source,
        MAX(updated_at) AS latest_update,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN updated_at >= CURRENT_TIMESTAMP() - INTERVAL '24 hours' THEN 1 END) AS recent_updates,
        DATEDIFF(minute, MAX(updated_at), CURRENT_TIMESTAMP()) AS minutes_since_update
    FROM {{ ref('tbl_dim_customer') }}
),

sla_thresholds AS (
    SELECT 
        table_name,
        data_source,
        CASE 
            WHEN table_name = 'fact_vehicle_telemetry' THEN {{ var("critical_freshness_hours") }} * 60
            WHEN table_name IN ('fact_shipments', 'dim_weather') THEN {{ var("data_freshness_hours") }} * 60
            ELSE 24 * 60  -- 24 hours for dimension tables
        END AS sla_minutes
    FROM source_freshness
)

SELECT 
    sf.table_name,
    sf.data_source,
    sf.latest_update,
    sf.total_records,
    sf.recent_updates,
    sf.minutes_since_update,
    st.sla_minutes,
    
    -- Freshness status
    CASE 
        WHEN sf.minutes_since_update <= st.sla_minutes THEN 'FRESH'
        WHEN sf.minutes_since_update <= st.sla_minutes * 1.5 THEN 'WARNING'
        ELSE 'STALE'
    END AS freshness_status,
    
    -- SLA compliance
    CASE 
        WHEN sf.minutes_since_update <= st.sla_minutes THEN TRUE
        ELSE FALSE
    END AS sla_compliant,
    
    -- Health score (0-100)
    GREATEST(0, LEAST(100, 
        100 - (sf.minutes_since_update - st.sla_minutes) / st.sla_minutes * 100
    )) AS freshness_health_score,
    
    -- Alert priority
    CASE 
        WHEN sf.minutes_since_update > st.sla_minutes * 2 THEN 'CRITICAL'
        WHEN sf.minutes_since_update > st.sla_minutes * 1.5 THEN 'HIGH'
        WHEN sf.minutes_since_update > st.sla_minutes THEN 'MEDIUM'
        ELSE 'LOW'
    END AS alert_priority,
    
    CURRENT_TIMESTAMP() AS check_timestamp

FROM source_freshness sf
JOIN sla_thresholds st ON sf.table_name = st.table_name
