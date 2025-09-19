- 4. Predictive Maintenance Features
-- File: models/analytics/ml_features/view_predictive_maintenance_features.sql
{{ config(
    materialized='table',
    tags=['ml', 'features', 'maintenance']
) }}

WITH vehicle_usage_patterns AS (
    SELECT 
        fvt.vehicle_id,
        DATE(fvt.timestamp) AS usage_date,
        dv.vehicle_type,
        dv.year AS manufacture_year,
        dv.odometer_km,
        dv.condition_score,
        
        -- Daily usage metrics
        COUNT(*) AS daily_data_points,
        AVG(fvt.speed_kmh) AS avg_speed,
        MAX(fvt.speed_kmh) AS max_speed,
        SUM(fvt.harsh_braking_events) AS daily_harsh_braking,
        SUM(fvt.harsh_acceleration_events) AS daily_harsh_acceleration,
        SUM(fvt.speeding_events) AS daily_speeding,
        SUM(fvt.idle_time_minutes) AS daily_idle_time,
        AVG(fvt.engine_temp_c) AS avg_engine_temp,
        MAX(fvt.engine_temp_c) AS max_engine_temp,
        AVG(fvt.fuel_consumption_lph) AS avg_fuel_consumption,
        AVG(fvt.engine_health_score) AS avg_engine_health,
        MAX(fvt.odometer_km) - MIN(fvt.odometer_km) AS daily_mileage
        
    FROM {{ ref('fact_vehicle_telemetry') }} fvt
    JOIN {{ ref('dim_vehicle') }} dv ON fvt.vehicle_id = dv.vehicle_id
    WHERE fvt.timestamp >= CURRENT_DATE() - 180  -- Last 6 months
    GROUP BY 1,2,3,4,5,6
),

maintenance_history AS (
    SELECT 
        vehicle_id,
        COUNT(*) AS total_services,
        AVG(cost) AS avg_service_cost,
        SUM(cost) AS total_service_cost,
        MAX(completed_date) AS last_service_date,
        DATEDIFF(day, MAX(completed_date), CURRENT_DATE()) AS days_since_service,
        AVG(DATEDIFF(day, LAG(completed_date) OVER (PARTITION BY vehicle_id ORDER BY completed_date), completed_date)) AS avg_service_interval
    FROM {{ ref('dim_vehicle_maintenance') }}
    WHERE completed_date IS NOT NULL
    GROUP BY 1
),

predictive_features AS (
    SELECT 
        vup.vehicle_id,
        vup.usage_date,
        vup.vehicle_type,
        vup.manufacture_year,
        2024 - vup.manufacture_year AS vehicle_age,
        vup.condition_score,
        
        -- Usage pattern features
        vup.daily_mileage,
        vup.avg_speed,
        vup.max_speed,
        vup.daily_harsh_braking,
        vup.daily_harsh_acceleration,
        vup.daily_speeding,
        vup.daily_idle_time,
        vup.avg_engine_temp,
        vup.max_engine_temp,
        vup.avg_fuel_consumption,
        vup.avg_engine_health,
        
        -- Calculated risk factors
        (vup.daily_harsh_braking + vup.daily_harsh_acceleration) AS total_harsh_events,
        vup.daily_speeding / NULLIF(vup.daily_data_points, 0) AS speeding_frequency,
        vup.daily_idle_time / NULLIF(vup.daily_data_points * 60, 0) AS idle_ratio,
        
        -- External factors from weather (simplified)
        CASE WHEN vup.usage_date IN (
            SELECT date FROM {{ ref('dim_weather') }} 
            WHERE condition IN ('Heavy Rain', 'Storm', 'Fog')
        ) THEN 1 ELSE 0 END AS harsh_weather_day,
        
        -- Maintenance history features
        mh.days_since_service,
        mh.avg_service_interval,
        mh.avg_service_cost,
        mh.total_services,
        
        -- Rolling averages for trend detection
        AVG(vup.avg_engine_health) OVER (
            PARTITION BY vup.vehicle_id 
            ORDER BY vup.usage_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS engine_health_7d_avg,
        
        AVG(vup.avg_engine_health) OVER (
            PARTITION BY vup.vehicle_id 
            ORDER BY vup.usage_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS engine_health_30d_avg,
        
        -- Degradation indicators
        LAG(vup.avg_engine_health, 7) OVER (
            PARTITION BY vup.vehicle_id ORDER BY vup.usage_date
        ) AS engine_health_7d_ago,
        
        LAG(vup.avg_fuel_consumption, 7) OVER (
            PARTITION BY vup.vehicle_id ORDER BY vup.usage_date
        ) AS fuel_consumption_7d_ago
        
    FROM vehicle_usage_patterns vup
    LEFT JOIN maintenance_history mh ON vup.vehicle_id = mh.vehicle_id
)

SELECT 
    vehicle_id,
    usage_date,
    vehicle_type,
    vehicle_age,
    condition_score,
    
    -- Usage intensity features
    daily_mileage,
    avg_speed,
    max_speed,
    total_harsh_events,
    speeding_frequency,
    idle_ratio,
    harsh_weather_day,
    
    -- Performance degradation features
    avg_engine_health,
    engine_health_7d_avg,
    engine_health_30d_avg,
    CASE 
        WHEN engine_health_7d_ago IS NOT NULL 
        THEN (avg_engine_health - engine_health_7d_ago) / 7.0 
        ELSE NULL 
    END AS engine_health_trend_daily,
    
    avg_fuel_consumption,
    CASE 
        WHEN fuel_consumption_7d_ago IS NOT NULL 
        THEN (avg_fuel_consumption - fuel_consumption_7d_ago) / 7.0 
        ELSE NULL 
    END AS fuel_efficiency_trend_daily,
    
    -- Maintenance predictors
    days_since_service,
    avg_service_interval,
    CASE 
        WHEN days_since_service > avg_service_interval * 1.2 THEN 1 
        ELSE 0 
    END AS overdue_service_flag,
    
    -- Risk scoring (0-10 scale, higher = more risk)
    LEAST(10, GREATEST(0,
        (10 - avg_engine_health) * 0.3 +
        (vehicle_age / 10.0) * 0.2 +
        (total_harsh_events / 10.0) * 0.2 +
        (days_since_service / 365.0 * 10) * 0.2 +
        (CASE WHEN avg_engine_health < engine_health_30d_avg * 0.95 THEN 1 ELSE 0 END) * 0.1
    )) AS breakdown_risk_score,
    
    -- Target variables for ML (next 30 days)
    LEAD(avg_engine_health, 30) OVER (
        PARTITION BY vehicle_id ORDER BY usage_date
    ) < avg_engine_health * 0.9 AS will_degrade_30d,
    
    EXISTS(
        SELECT 1 FROM {{ ref('dim_vehicle_maintenance') }} dvm
        WHERE dvm.vehicle_id = predictive_features.vehicle_id
        AND dvm.scheduled_date BETWEEN usage_date AND usage_date + 30
    ) AS has_scheduled_maintenance_30d

FROM predictive_features

---