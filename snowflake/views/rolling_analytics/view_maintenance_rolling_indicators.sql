-- Rolling Maintenance Indicators View
-- This view references the dbt model to ensure consistency and eliminate redundancy
-- Updated to use dbt model as single source of truth

CREATE OR REPLACE VIEW view_maintenance_rolling_indicators AS
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
    
    -- Rolling 30-day metrics
    maintenance_events_30d,
    maintenance_cost_30d,
    avg_risk_score_30d,
    
    -- Rolling 90-day metrics
    maintenance_events_90d,
    maintenance_cost_90d,
    avg_risk_score_90d,
    
    -- Rolling 365-day metrics
    maintenance_events_365d,
    maintenance_cost_365d,
    avg_risk_score_365d,
    
    -- Maintenance frequency analysis
    maintenance_frequency_30d_category,
    maintenance_risk_level_30d,
    avg_cost_per_maintenance_90d,
    predictive_maintenance_score,
    maintenance_efficiency_score

FROM LOGISTICS_DW_PROD.MARTS.ML_MAINTENANCE_ROLLING_INDICATORS
WHERE maintenance_date >= DATEADD('day', -365, CURRENT_DATE())
ORDER BY vehicle_id, maintenance_date DESC