-- Segments deliveries by distance/duration for different ML models
SELECT 
    shipment_id,
    CASE 
        WHEN route_distance_km <= 50 THEN 'short_haul'
        WHEN route_distance_km <= 200 THEN 'medium_haul' 
        ELSE 'long_haul'
    END AS haul_type,
    
    CASE 
        WHEN delivery_duration_hours <= 4 THEN 'same_day'
        WHEN delivery_duration_hours <= 24 THEN 'next_day'
        ELSE 'multi_day'
    END AS delivery_window,
    
    -- Different features matter for different haul types
    route_complexity_score,
    traffic_impact_factor,
    weather_delay_probability