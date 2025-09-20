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
        MAX(shipment_date) AS latest_update,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN shipment_date >= CURRENT_DATE() - INTERVAL '{{ var("data_freshness_hours", 24) }} hours' THEN 1 END) AS recent_updates,
        DATEDIFF(minute, MAX(shipment_date), CURRENT_TIMESTAMP()) AS minutes_since_update
    FROM {{ ref('tbl_fact_shipments') }}
    
    UNION ALL
    
    SELECT 
        'fact_vehicle_telemetry' AS table_name,
        'vehicle_telematics' AS data_source,
        MAX(timestamp) AS latest_update,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN timestamp >= CURRENT_TIMESTAMP() - INTERVAL '{{ var("critical_freshness_hours", 1) }} hours' THEN 1 END) AS recent_updates,
        DATEDIFF(minute, MAX(timestamp), CURRENT_TIMESTAMP()) AS minutes_since_update
    FROM {{ ref('tbl_fact_vehicle_telemetry') }}
    
    UNION ALL
    
    SELECT 
        'fact_route_conditions' AS table_name,
        'route_conditions' AS data_source,
        MAX(to_date(cast(date_key as string), 'YYYYMMDD')) AS latest_update,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN to_date(cast(date_key as string), 'YYYYMMDD') >= CURRENT_DATE() - INTERVAL '{{ var("data_freshness_hours", 24) }} hours' THEN 1 END) AS recent_updates,
        DATEDIFF(minute, MAX(to_date(cast(date_key as string), 'YYYYMMDD')), CURRENT_TIMESTAMP()) AS minutes_since_update
    FROM {{ ref('tbl_fact_route_conditions') }}
    
    UNION ALL
    
    SELECT 
        'fact_route_performance' AS table_name,
        'route_performance' AS data_source,
        MAX(to_date(cast(date_key as string), 'YYYYMMDD')) AS latest_update,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN to_date(cast(date_key as string), 'YYYYMMDD') >= CURRENT_DATE() - INTERVAL '{{ var("data_freshness_hours", 24) }} hours' THEN 1 END) AS recent_updates,
        DATEDIFF(minute, MAX(to_date(cast(date_key as string), 'YYYYMMDD')), CURRENT_TIMESTAMP()) AS minutes_since_update
    FROM {{ ref('tbl_fact_route_performance') }}
    
    UNION ALL
    
    SELECT 
        'fact_vehicle_utilization' AS table_name,
        'vehicle_utilization' AS data_source,
        MAX(to_date(cast(date_key as string), 'YYYYMMDD')) AS latest_update,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN to_date(cast(date_key as string), 'YYYYMMDD') >= CURRENT_DATE() - INTERVAL '{{ var("data_freshness_hours", 24) }} hours' THEN 1 END) AS recent_updates,
        DATEDIFF(minute, MAX(to_date(cast(date_key as string), 'YYYYMMDD')), CURRENT_TIMESTAMP()) AS minutes_since_update
    FROM {{ ref('tbl_fact_vehicle_utilization') }}
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
