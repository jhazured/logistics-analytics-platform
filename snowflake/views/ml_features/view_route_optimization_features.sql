-- Features for dynamic routing ML models
SELECT 
    route_id,
    haul_type,
    
    -- Time-based features
    hour_of_day,
    day_of_week,
    is_peak_season,
    
    -- Historical performance
    avg_delivery_time_minutes,
    delivery_success_rate,
    avg_fuel_cost_per_km,
    
    -- Real-time conditions
    current_weather_severity,
    traffic_density_score,
    road_condition_index,
    
    -- Vehicle suitability
    vehicle_capacity_utilization,
    vehicle_maintenance_urgency,
    driver_experience_level