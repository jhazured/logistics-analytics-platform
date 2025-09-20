-- Test route distance positivity
-- Validates that route distances are positive and reasonable

WITH route_distance_validation AS (
    SELECT 
        route_id,
        origin_location_id,
        destination_location_id,
        distance_km,
        estimated_travel_time_hours,
        route_type,
        
        -- Check for negative distances
        CASE 
            WHEN distance_km < 0 THEN 1 
            ELSE 0 
        END as negative_distance,
        
        -- Check for zero distances
        CASE 
            WHEN distance_km = 0 THEN 1 
            ELSE 0 
        END as zero_distance,
        
        -- Check for extremely long distances (possible data error)
        CASE 
            WHEN distance_km > 8000 THEN 1 
            ELSE 0 
        END as extremely_long_distance,
        
        -- Check for missing distances
        CASE 
            WHEN distance_km IS NULL THEN 1 
            ELSE 0 
        END as missing_distance,
        
        -- Check for unreasonable travel times
        CASE 
            WHEN estimated_travel_time_hours < 0 THEN 1 
            ELSE 0 
        END as negative_travel_time,
        
        -- Check for extremely long travel times
        CASE 
            WHEN estimated_travel_time_hours > 48 THEN 1 
            ELSE 0 
        END as extremely_long_travel_time,
        
        -- Check for missing travel times
        CASE 
            WHEN estimated_travel_time_hours IS NULL THEN 1 
            ELSE 0 
        END as missing_travel_time,
        
        -- Calculate expected travel time based on distance and route type
        CASE 
            WHEN route_type = 'HIGHWAY' THEN distance_km / 100.0
            WHEN route_type = 'URBAN' THEN distance_km / 40.0
            WHEN route_type = 'RURAL' THEN distance_km / 65.0
            ELSE distance_km / 55.0
        END as expected_travel_time,
        
        -- Check if actual travel time is reasonable compared to expected
        CASE 
            WHEN ABS(estimated_travel_time_hours - (distance_km / 55.0)) > distance_km / 55.0 * 0.5 THEN 1 
            ELSE 0 
        END as unreasonable_travel_time
        
    FROM {{ ref('dim_route') }}
    WHERE is_active = true
)

SELECT 
    route_id,
    origin_location_id,
    destination_location_id,
    distance_km,
    estimated_travel_time_hours,
    route_type,
    expected_travel_time,
    negative_distance,
    zero_distance,
    extremely_long_distance,
    missing_distance,
    negative_travel_time,
    extremely_long_travel_time,
    missing_travel_time,
    unreasonable_travel_time
FROM route_distance_validation
WHERE 
    negative_distance = 1
    OR zero_distance = 1
    OR extremely_long_distance = 1
    OR missing_distance = 1
    OR negative_travel_time = 1
    OR extremely_long_travel_time = 1
    OR missing_travel_time = 1
    OR unreasonable_travel_time = 1
