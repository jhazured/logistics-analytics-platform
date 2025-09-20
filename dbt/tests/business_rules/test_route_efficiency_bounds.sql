-- Test that route efficiency scores are calculated correctly
SELECT 
    shipment_id,
    route_efficiency_score,
    distance_km,
    planned_duration_minutes,
    actual_duration_minutes
FROM {{ ref('fact_shipments') }}
WHERE 
    -- Efficiency score should be between 0 and 100
    route_efficiency_score NOT BETWEEN 0 AND 100
    OR
    -- If actual equals planned, efficiency should be near 100
    (ABS(actual_duration_minutes - planned_duration_minutes) < 5 
     AND route_efficiency_score < 95)
    OR
    -- If actual is much worse than planned, efficiency should be low
    (actual_duration_minutes > planned_duration_minutes * 1.5
     AND route_efficiency_score > 50)