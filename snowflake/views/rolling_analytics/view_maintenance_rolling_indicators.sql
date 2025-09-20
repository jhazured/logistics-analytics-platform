-- Rolling maintenance indicators view
-- Provides comprehensive maintenance analytics with rolling windows for predictive maintenance

CREATE OR REPLACE VIEW view_maintenance_rolling_indicators AS
WITH maintenance_data AS (
    SELECT 
        vehicle_id,
        maintenance_date,
        maintenance_type,
        maintenance_cost_usd,
        maintenance_duration_hours,
        risk_score,
        maintenance_status
    FROM {{ ref('dim_vehicle_maintenance') }}
),

vehicle_data AS (
    SELECT 
        vehicle_id,
        vehicle_number,
        make,
        model,
        model_year,
        current_mileage,
        maintenance_interval_miles,
        fuel_efficiency_mpg
    FROM {{ ref('dim_vehicle') }}
),

maintenance_metrics AS (
    SELECT 
        m.vehicle_id,
        v.vehicle_number,
        v.make,
        v.model,
        v.model_year,
        v.current_mileage,
        v.maintenance_interval_miles,
        v.fuel_efficiency_mpg,
        m.maintenance_date,
        m.maintenance_type,
        m.maintenance_cost_usd,
        m.maintenance_duration_hours,
        m.risk_score,
        m.maintenance_status,
        -- Calculate days since last maintenance
        DATEDIFF(day, LAG(m.maintenance_date) OVER (PARTITION BY m.vehicle_id ORDER BY m.maintenance_date), m.maintenance_date) as days_since_previous_maintenance,
        -- Calculate miles since last maintenance (using current mileage as proxy)
        v.current_mileage - LAG(v.current_mileage) OVER (PARTITION BY m.vehicle_id ORDER BY m.maintenance_date) as miles_since_previous_maintenance
    FROM maintenance_data m
    JOIN vehicle_data v ON m.vehicle_id = v.vehicle_id
),

rolling_maintenance AS (
    SELECT 
        vehicle_id,
        vehicle_number,
        make,
        model,
        model_year,
        current_mileage,
        maintenance_interval_miles,
        fuel_efficiency_mpg,
        maintenance_date,
        maintenance_type,
        maintenance_cost_usd,
        maintenance_duration_hours,
        risk_score,
        maintenance_status,
        days_since_previous_maintenance,
        miles_since_previous_maintenance,
        
        -- Rolling 30-day maintenance metrics
        COUNT(*) OVER (
            PARTITION BY vehicle_id 
            ORDER BY maintenance_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as rolling_30d_maintenance_count,
        
        SUM(maintenance_cost_usd) OVER (
            PARTITION BY vehicle_id 
            ORDER BY maintenance_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as rolling_30d_maintenance_cost,
        
        AVG(maintenance_duration_hours) OVER (
            PARTITION BY vehicle_id 
            ORDER BY maintenance_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as rolling_30d_avg_duration,
        
        AVG(risk_score) OVER (
            PARTITION BY vehicle_id 
            ORDER BY maintenance_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as rolling_30d_avg_risk_score,
        
        -- Rolling 90-day maintenance metrics
        COUNT(*) OVER (
            PARTITION BY vehicle_id 
            ORDER BY maintenance_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as rolling_90d_maintenance_count,
        
        SUM(maintenance_cost_usd) OVER (
            PARTITION BY vehicle_id 
            ORDER BY maintenance_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as rolling_90d_maintenance_cost,
        
        AVG(maintenance_duration_hours) OVER (
            PARTITION BY vehicle_id 
            ORDER BY maintenance_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as rolling_90d_avg_duration,
        
        AVG(risk_score) OVER (
            PARTITION BY vehicle_id 
            ORDER BY maintenance_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as rolling_90d_avg_risk_score,
        
        -- Rolling 365-day maintenance metrics
        COUNT(*) OVER (
            PARTITION BY vehicle_id 
            ORDER BY maintenance_date 
            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        ) as rolling_365d_maintenance_count,
        
        SUM(maintenance_cost_usd) OVER (
            PARTITION BY vehicle_id 
            ORDER BY maintenance_date 
            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        ) as rolling_365d_maintenance_cost,
        
        AVG(maintenance_duration_hours) OVER (
            PARTITION BY vehicle_id 
            ORDER BY maintenance_date 
            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        ) as rolling_365d_avg_duration,
        
        AVG(risk_score) OVER (
            PARTITION BY vehicle_id 
            ORDER BY maintenance_date 
            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        ) as rolling_365d_avg_risk_score
        
    FROM maintenance_metrics
)

SELECT 
    vehicle_id,
    vehicle_number,
    make,
    model,
    model_year,
    current_mileage,
    maintenance_interval_miles,
    fuel_efficiency_mpg,
    maintenance_date,
    maintenance_type,
    maintenance_cost_usd,
    maintenance_duration_hours,
    risk_score,
    maintenance_status,
    days_since_previous_maintenance,
    miles_since_previous_maintenance,
    
    -- Rolling metrics
    rolling_30d_maintenance_count,
    rolling_30d_maintenance_cost,
    rolling_30d_avg_duration,
    rolling_30d_avg_risk_score,
    rolling_90d_maintenance_count,
    rolling_90d_maintenance_cost,
    rolling_90d_avg_duration,
    rolling_90d_avg_risk_score,
    rolling_365d_maintenance_count,
    rolling_365d_maintenance_cost,
    rolling_365d_avg_duration,
    rolling_365d_avg_risk_score,
    
    -- Calculated indicators
    CASE 
        WHEN rolling_30d_maintenance_count > 3 THEN 'HIGH_FREQUENCY'
        WHEN rolling_30d_maintenance_count > 1 THEN 'MEDIUM_FREQUENCY'
        ELSE 'LOW_FREQUENCY'
    END as maintenance_frequency_30d,
    
    CASE 
        WHEN rolling_30d_avg_risk_score > 80 THEN 'HIGH_RISK'
        WHEN rolling_30d_avg_risk_score > 50 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END as maintenance_risk_level_30d,
    
    -- Cost per mile indicators
    CASE 
        WHEN rolling_90d_maintenance_cost > 0 AND rolling_90d_maintenance_count > 0
        THEN rolling_90d_maintenance_cost / rolling_90d_maintenance_count
        ELSE 0
    END as avg_cost_per_maintenance_90d,
    
    -- Predictive maintenance score
    CASE 
        WHEN rolling_30d_avg_risk_score > 80 THEN 100
        WHEN rolling_30d_avg_risk_score > 60 THEN 80
        WHEN rolling_30d_avg_risk_score > 40 THEN 60
        WHEN rolling_30d_avg_risk_score > 20 THEN 40
        ELSE 20
    END as predictive_maintenance_score,
    
    -- Maintenance efficiency score
    CASE 
        WHEN rolling_30d_avg_duration < 2 THEN 100
        WHEN rolling_30d_avg_duration < 4 THEN 80
        WHEN rolling_30d_avg_duration < 6 THEN 60
        WHEN rolling_30d_avg_duration < 8 THEN 40
        ELSE 20
    END as maintenance_efficiency_score

FROM rolling_maintenance
ORDER BY vehicle_id, maintenance_date;
