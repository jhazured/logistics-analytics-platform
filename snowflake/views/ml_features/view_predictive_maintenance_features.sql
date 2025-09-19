-- Vehicle health prediction dataset
SELECT 
    vehicle_id,
    
    -- Usage patterns
    miles_since_service,
    avg_daily_miles_last_30d,
    harsh_braking_events,
    engine_idle_time_pct,
    
    -- Performance degradation
    fuel_efficiency_trend,
    engine_diagnostic_scores,
    tire_wear_indicators,
    
    -- External factors
    avg_weather_harshness,
    route_difficulty_score,
    
    -- Target variables for ML
    days_until_next_service,
    breakdown_risk_score