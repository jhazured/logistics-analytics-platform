-- =====================================================
-- Rolling Window Analytics Views
-- =====================================================

--- Maintenance Rolling Indicators

{{ config(
    materialized='table',
    tags=['analytics', 'rolling', 'maintenance']
) }}

WITH vehicle_daily_usage AS (
    SELECT 
        fvt.vehicle_id,
        DATE(fvt.timestamp) AS usage_date,
        dv.vehicle_type,
        dv.year AS vehicle_year,
        dv.condition_score,
        
        -- Daily usage metrics
        COUNT(*) AS daily_telemetry_points,
        MAX(fvt.odometer_km) - MIN(fvt.odometer_km) AS daily_mileage,
        AVG(fvt.engine_health_score) AS avg_engine_health,
        AVG(fvt.fuel_consumption_lph) AS avg_fuel_consumption,
        SUM(fvt.harsh_braking_events) AS daily_harsh_braking,
        SUM(fvt.harsh_acceleration_events) AS daily_harsh_acceleration,
        SUM(fvt.speeding_events) AS daily_speeding_events,
        SUM(fvt.idle_time_minutes) AS daily_idle_time,
        AVG(fvt.engine_temp_c) AS avg_engine_temp,
        MAX(fvt.engine_temp_c) AS max_engine_temp,
        
        -- Diagnostic indicators
        SUM(CASE WHEN fvt.maintenance_alert THEN 1 ELSE 0 END) AS daily_maintenance_alerts,
        COUNT(DISTINCT PARSE_JSON(fvt.diagnostic_codes)) AS unique_diagnostic_codes
        
    FROM {{ ref('fact_vehicle_telemetry') }} fvt
    JOIN {{ ref('dim_vehicle') }} dv ON fvt.vehicle_id = dv.vehicle_id
    WHERE fvt.timestamp >= CURRENT_DATE() - 180  -- Last 6 months
    GROUP BY 1, 2, 3, 4, 5
),

maintenance_history AS (
    SELECT 
        dvm.vehicle_id,
        dvm.completed_date,
        dvm.maintenance_type,
        dvm.cost,
        LAG(dvm.completed_date) OVER (PARTITION BY dvm.vehicle_id ORDER BY dvm.completed_date) AS previous_service_date,
        DATEDIFF(day, LAG(dvm.completed_date) OVER (PARTITION BY dvm.vehicle_id ORDER BY dvm.completed_date), dvm.completed_date) AS days_between_service
    FROM {{ ref('dim_vehicle_maintenance') }} dvm
    WHERE dvm.completed_date IS NOT NULL
),

rolling_maintenance_metrics AS (
    SELECT 
        vdu.vehicle_id,
        vdu.usage_date,
        vdu.vehicle_type,
        vdu.vehicle_year,
        vdu.condition_score,
        
        -- Current day metrics
        vdu.daily_mileage,
        vdu.avg_engine_health,
        vdu.avg_fuel_consumption,
        vdu.daily_harsh_braking,
        vdu.daily_harsh_acceleration,
        vdu.daily_speeding_events,
        vdu.daily_idle_time,
        vdu.avg_engine_temp,
        vdu.max_engine_temp,
        vdu.daily_maintenance_alerts,
        
        -- 30-day rolling windows for maintenance tracking
        SUM(vdu.daily_mileage) OVER (
            PARTITION BY vdu.vehicle_id 
            ORDER BY vdu.usage_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS mileage_30d,
        
        AVG(vdu.avg_engine_health) OVER (
            PARTITION BY vdu.vehicle_id 
            ORDER BY vdu.usage_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS engine_health_30d_avg,
        
        AVG(vdu.avg_fuel_consumption) OVER (
            PARTITION BY vdu.vehicle_id 
            ORDER BY vdu.usage_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS fuel_consumption_30d_avg,
        
        SUM(vdu.daily_harsh_braking) OVER (
            PARTITION BY vdu.vehicle_id 
            ORDER BY vdu.usage_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS harsh_braking_30d,
        
        SUM(vdu.daily_maintenance_alerts) OVER (
            PARTITION BY vdu.vehicle_id 
            ORDER BY vdu.usage_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS maintenance_alerts_30d,
        
        -- 14-day engine health trending
        AVG(vdu.avg_engine_health) OVER (
            PARTITION BY vdu.vehicle_id 
            ORDER BY vdu.usage_date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS engine_health_14d_avg,
        
        AVG(vdu.max_engine_temp) OVER (
            PARTITION BY vdu.vehicle_id 
            ORDER BY vdu.usage_date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS max_engine_temp_14d_avg,
        
        -- Degradation trend calculation
        AVG(vdu.avg_engine_health) OVER (
            PARTITION BY vdu.vehicle_id 
            ORDER BY vdu.usage_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS engine_health_7d_avg
        
    FROM vehicle_daily_usage vdu
)

SELECT 
    rmm.vehicle_id,
    rmm.usage_date,
    rmm.vehicle_type,
    rmm.vehicle_year,
    rmm.condition_score,
    
    -- Current metrics
    rmm.daily_mileage,
    rmm.avg_engine_health,
    rmm.avg_fuel_consumption,
    rmm.daily_harsh_braking,
    rmm.daily_maintenance_alerts,
    
    -- Rolling accumulations
    rmm.mileage_30d,
    rmm.engine_health_30d_avg,
    rmm.fuel_consumption_30d_avg,
    rmm.harsh_braking_30d,
    rmm.maintenance_alerts_30d,
    
    -- Engine health trending
    rmm.engine_health_14d_avg,
    rmm.max_engine_temp_14d_avg,
    rmm.engine_health_7d_avg,
    
    -- Maintenance interval tracking
    mh.days_between_service AS last_service_interval_days,
    DATEDIFF(day, mh.completed_date, rmm.usage_date) AS days_since_last_service,
    
    -- Service interval recommendations based on mileage accumulation
    CASE 
        WHEN rmm.mileage_30d >= 5000 THEN 'service_due_high_mileage'
        WHEN rmm.mileage_30d >= 3000 THEN 'service_due_moderate_mileage'
        WHEN DATEDIFF(day, mh.completed_date, rmm.usage_date) >= 90 THEN 'service_due_time_based'
        ELSE 'service_current'
    END AS service_recommendation,
    
    -- Health degradation indicators
    CASE 
        WHEN rmm.engine_health_7d_avg < rmm.engine_health_14d_avg * 0.95 THEN 'degrading'
        WHEN rmm.engine_health_7d_avg > rmm.engine_health_14d_avg * 1.05 THEN 'improving'
        ELSE 'stable'
    END AS engine_health_trend,
    
    -- Maintenance urgency scoring
    CASE 
        WHEN rmm.maintenance_alerts_30d >= 10 OR rmm.avg_engine_health < 6 THEN 'urgent'
        WHEN rmm.maintenance_alerts_30d >= 5 OR rmm.avg_engine_health < 7 THEN 'high'
        WHEN rmm.maintenance_alerts_30d >= 2 OR rmm.avg_engine_health < 8 THEN 'medium'
        ELSE 'low'
    END AS maintenance_urgency,
    
    -- Predictive maintenance score (0-10 scale)
    GREATEST(0, LEAST(10, 
        10 - (rmm.avg_engine_health * 0.4) -
        (rmm.maintenance_alerts_30d * 0.3) -
        (rmm.harsh_braking_30d / 100.0 * 0.2) -
        (CASE WHEN DATEDIFF(day, mh.completed_date, rmm.usage_date) > 90 THEN 1 ELSE 0 END * 0.1)
    )) AS maintenance_risk_score

FROM rolling_maintenance_metrics rmm
LEFT JOIN maintenance_history mh ON rmm.vehicle_id = mh.vehicle_id 
    AND mh.completed_date = (
        SELECT MAX(completed_date) 
        FROM maintenance_history mh2 
        WHERE mh2.vehicle_id = rmm.vehicle_id 
        AND mh2.completed_date <= rmm.usage_date
    )
