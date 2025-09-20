-- =====================================================
-- Consolidated Maintenance Features Model
-- Combines rolling indicators and predictive maintenance features
-- =====================================================

{{ config(
    materialized='table',
    cluster_by=['vehicle_id', 'feature_date'],
    tags=['marts', 'ml_features', 'ml', 'maintenance', 'predictive', 'consolidated', 'ml_optimized', 'load_third']
) }}

WITH vehicle_telemetry AS (
    SELECT * FROM {{ ref('tbl_fact_vehicle_telemetry') }}
),

maintenance_history AS (
    SELECT * FROM {{ ref('tbl_dim_vehicle_maintenance') }}
),

vehicles AS (
    SELECT * FROM {{ ref('tbl_dim_vehicle') }}
),

-- Daily telemetry aggregation
telemetry_aggregated AS (
    SELECT
        vehicle_id,
        DATE(timestamp) AS telemetry_date,
        AVG(engine_temp_c) AS avg_engine_temp,
        AVG(fuel_level_percent) AS avg_fuel_level,
        AVG(engine_rpm) AS avg_engine_rpm,
        SUM(harsh_braking_events) AS total_harsh_braking,
        SUM(harsh_acceleration_events) AS total_harsh_acceleration,
        SUM(speeding_events) AS total_speeding_events,
        AVG(idle_time_minutes) AS avg_idle_time,
        AVG(engine_health_score) AS avg_engine_health,
        COUNT(*) AS telemetry_records_count,
        -- Additional telemetry metrics
        MAX(engine_temp_c) AS max_engine_temp,
        AVG(fuel_consumption_lph) AS avg_fuel_consumption,
        SUM(CASE WHEN maintenance_alert THEN 1 ELSE 0 END) AS daily_maintenance_alerts,
        COUNT(DISTINCT diagnostic_codes) AS unique_diagnostic_codes
    FROM vehicle_telemetry
    WHERE timestamp >= CURRENT_DATE() - 180  -- Last 6 months
    GROUP BY vehicle_id, DATE(timestamp)
),

-- Maintenance history with intervals
maintenance_with_intervals AS (
    SELECT 
        vehicle_id,
        maintenance_date as completed_date,
        maintenance_type,
        maintenance_cost_usd as cost,
        maintenance_mileage,
        LAG(maintenance_date) OVER (PARTITION BY vehicle_id ORDER BY maintenance_date) AS previous_service_date,
        DATEDIFF(day, LAG(maintenance_date) OVER (PARTITION BY vehicle_id ORDER BY maintenance_date), maintenance_date) AS days_between_service
    FROM maintenance_history
    WHERE completed_date IS NOT NULL
),

-- Rolling maintenance indicators
maintenance_rolling AS (
    SELECT
        v.vehicle_id,
        v.vehicle_type,
        v.make,
        v.model,
        v.model_year,
        v.current_mileage,
        v.maintenance_interval_miles,
        v.last_maintenance_date,
        v.next_maintenance_date,
        
        -- Rolling 7-day maintenance indicators
        COUNT(CASE WHEN m.completed_date >= CURRENT_DATE() - 7 THEN 1 END) AS maintenance_events_7d,
        SUM(CASE WHEN m.completed_date >= CURRENT_DATE() - 7 THEN m.cost ELSE 0 END) AS maintenance_cost_7d,
        
        -- Rolling 30-day maintenance indicators
        COUNT(CASE WHEN m.completed_date >= CURRENT_DATE() - 30 THEN 1 END) AS maintenance_events_30d,
        SUM(CASE WHEN m.completed_date >= CURRENT_DATE() - 30 THEN m.cost ELSE 0 END) AS maintenance_cost_30d,
        
        -- Rolling 90-day maintenance indicators
        COUNT(CASE WHEN m.completed_date >= CURRENT_DATE() - 90 THEN 1 END) AS maintenance_events_90d,
        SUM(CASE WHEN m.completed_date >= CURRENT_DATE() - 90 THEN m.cost ELSE 0 END) AS maintenance_cost_90d,
        
        -- Maintenance urgency indicators
        CASE 
            WHEN v.current_mileage - COALESCE(MAX(m.maintenance_mileage), 0) > v.maintenance_interval_miles * 1.2 THEN 'CRITICAL'
            WHEN v.current_mileage - COALESCE(MAX(m.maintenance_mileage), 0) > v.maintenance_interval_miles * 1.1 THEN 'HIGH'
            WHEN v.current_mileage - COALESCE(MAX(m.maintenance_mileage), 0) > v.maintenance_interval_miles * 1.0 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS maintenance_urgency,
        
        -- Days since last maintenance
        DATEDIFF(day, MAX(m.completed_date), CURRENT_DATE()) AS days_since_last_maintenance,
        
        -- Miles since last maintenance
        v.current_mileage - COALESCE(MAX(m.maintenance_mileage), 0) AS miles_since_last_maintenance,
        
        -- Last service interval
        MAX(m.days_between_service) AS last_service_interval_days
        
    FROM vehicles v
    LEFT JOIN maintenance_with_intervals m ON v.vehicle_id = m.vehicle_id
    GROUP BY v.vehicle_id, v.vehicle_type, v.make, v.model, v.model_year, 
             v.current_mileage, v.maintenance_interval_miles, v.last_maintenance_date, v.next_maintenance_date
),

-- Rolling telemetry indicators
telemetry_rolling AS (
    SELECT
        vehicle_id,
        telemetry_date,
        
        -- Current day metrics
        avg_engine_temp,
        avg_fuel_level,
        avg_engine_rpm,
        total_harsh_braking,
        total_harsh_acceleration,
        total_speeding_events,
        avg_idle_time,
        avg_engine_health,
        max_engine_temp,
        avg_fuel_consumption,
        daily_maintenance_alerts,
        unique_diagnostic_codes,
        
        -- Rolling 7-day telemetry indicators
        AVG(avg_engine_temp) OVER (
            PARTITION BY vehicle_id 
            ORDER BY telemetry_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS avg_engine_temp_7d,
        AVG(avg_engine_health) OVER (
            PARTITION BY vehicle_id 
            ORDER BY telemetry_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS avg_engine_health_7d,
        SUM(total_harsh_braking) OVER (
            PARTITION BY vehicle_id 
            ORDER BY telemetry_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS harsh_braking_7d,
        SUM(total_harsh_acceleration) OVER (
            PARTITION BY vehicle_id 
            ORDER BY telemetry_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS harsh_acceleration_7d,
        SUM(daily_maintenance_alerts) OVER (
            PARTITION BY vehicle_id 
            ORDER BY telemetry_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS maintenance_alerts_7d,
        
        -- Rolling 14-day telemetry indicators
        AVG(avg_engine_health) OVER (
            PARTITION BY vehicle_id 
            ORDER BY telemetry_date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS engine_health_14d_avg,
        AVG(max_engine_temp) OVER (
            PARTITION BY vehicle_id 
            ORDER BY telemetry_date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS max_engine_temp_14d_avg,
        
        -- Rolling 30-day telemetry indicators
        AVG(avg_engine_temp) OVER (
            PARTITION BY vehicle_id 
            ORDER BY telemetry_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS avg_engine_temp_30d,
        AVG(avg_engine_health) OVER (
            PARTITION BY vehicle_id 
            ORDER BY telemetry_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS avg_engine_health_30d,
        SUM(total_harsh_braking) OVER (
            PARTITION BY vehicle_id 
            ORDER BY telemetry_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS harsh_braking_30d,
        SUM(total_harsh_acceleration) OVER (
            PARTITION BY vehicle_id 
            ORDER BY telemetry_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS harsh_acceleration_30d,
        SUM(daily_maintenance_alerts) OVER (
            PARTITION BY vehicle_id 
            ORDER BY telemetry_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS maintenance_alerts_30d,
        
        -- Rolling 90-day telemetry indicators
        AVG(avg_engine_health) OVER (
            PARTITION BY vehicle_id 
            ORDER BY telemetry_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS avg_engine_health_90d,
        SUM(total_harsh_braking) OVER (
            PARTITION BY vehicle_id 
            ORDER BY telemetry_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS harsh_braking_90d,
        SUM(daily_maintenance_alerts) OVER (
            PARTITION BY vehicle_id 
            ORDER BY telemetry_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS maintenance_alerts_90d
        
    FROM telemetry_aggregated
)

SELECT
    mr.vehicle_id,
    mr.vehicle_type,
    mr.make,
    mr.model,
    mr.model_year,
    mr.current_mileage,
    mr.maintenance_interval_miles,
    mr.last_maintenance_date,
    mr.next_maintenance_date,
    tr.telemetry_date AS feature_date,
    
    -- Current telemetry metrics
    tr.avg_engine_temp,
    tr.avg_fuel_level,
    tr.avg_engine_rpm,
    tr.total_harsh_braking,
    tr.total_harsh_acceleration,
    tr.total_speeding_events,
    tr.avg_idle_time,
    tr.avg_engine_health,
    tr.max_engine_temp,
    tr.avg_fuel_consumption,
    tr.daily_maintenance_alerts,
    tr.unique_diagnostic_codes,
    
    -- Rolling maintenance metrics
    mr.maintenance_events_7d,
    mr.maintenance_cost_7d,
    mr.maintenance_events_30d,
    mr.maintenance_cost_30d,
    mr.maintenance_events_90d,
    mr.maintenance_cost_90d,
    mr.maintenance_urgency,
    mr.days_since_last_maintenance,
    mr.miles_since_last_maintenance,
    mr.last_service_interval_days,
    
    -- Rolling telemetry metrics
    tr.avg_engine_temp_7d,
    tr.avg_engine_health_7d,
    tr.harsh_braking_7d,
    tr.harsh_acceleration_7d,
    tr.maintenance_alerts_7d,
    tr.engine_health_14d_avg,
    tr.max_engine_temp_14d_avg,
    tr.avg_engine_temp_30d,
    tr.avg_engine_health_30d,
    tr.harsh_braking_30d,
    tr.harsh_acceleration_30d,
    tr.maintenance_alerts_30d,
    tr.avg_engine_health_90d,
    tr.harsh_braking_90d,
    tr.maintenance_alerts_90d,
    
    -- Engine health trending
    CASE 
        WHEN tr.avg_engine_health_7d < tr.engine_health_14d_avg * 0.95 THEN 'degrading'
        WHEN tr.avg_engine_health_7d > tr.engine_health_14d_avg * 1.05 THEN 'improving'
        ELSE 'stable'
    END AS engine_health_trend,
    
    -- Service recommendation
    CASE 
        WHEN mr.miles_since_last_maintenance >= mr.maintenance_interval_miles * 1.2 THEN 'service_due_high_mileage'
        WHEN mr.miles_since_last_maintenance >= mr.maintenance_interval_miles * 1.0 THEN 'service_due_moderate_mileage'
        WHEN mr.days_since_last_maintenance >= 90 THEN 'service_due_time_based'
        ELSE 'service_current'
    END AS service_recommendation,
    
    -- Predictive maintenance score (0-100 scale)
    CASE 
        WHEN mr.maintenance_urgency = 'CRITICAL' THEN 100
        WHEN mr.maintenance_urgency = 'HIGH' THEN 80
        WHEN mr.maintenance_urgency = 'MEDIUM' THEN 60
        WHEN tr.avg_engine_health_7d < 70 THEN 70
        WHEN tr.harsh_braking_7d > 10 OR tr.harsh_acceleration_7d > 10 THEN 50
        WHEN tr.maintenance_alerts_30d >= 10 THEN 75
        WHEN tr.maintenance_alerts_30d >= 5 THEN 60
        WHEN tr.maintenance_alerts_30d >= 2 THEN 40
        ELSE 20
    END AS predictive_maintenance_score,
    
    -- Maintenance risk score (0-10 scale)
    GREATEST(0, LEAST(10, 
        10 - (tr.avg_engine_health * 0.4) -
        (tr.maintenance_alerts_30d * 0.3) -
        (tr.harsh_braking_30d / 100.0 * 0.2) -
        (CASE WHEN mr.days_since_last_maintenance > 90 THEN 1 ELSE 0 END * 0.1)
    )) AS maintenance_risk_score,
    
    -- Next maintenance due prediction
    CASE 
        WHEN mr.miles_since_last_maintenance >= mr.maintenance_interval_miles THEN 0
        WHEN mr.days_since_last_maintenance >= 90 THEN 0
        ELSE GREATEST(
            (mr.maintenance_interval_miles - mr.miles_since_last_maintenance) / 100, -- Assuming 100 miles per day average
            (90 - mr.days_since_last_maintenance)
        )
    END AS next_maintenance_due_days

FROM maintenance_rolling mr
LEFT JOIN telemetry_rolling tr ON mr.vehicle_id = tr.vehicle_id
WHERE tr.telemetry_date IS NOT NULL
