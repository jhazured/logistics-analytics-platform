-- Test that route efficiency scores are calculated correctly
SELECT 
    shipment_id,
    route_efficiency_score,
    actual_distance_miles,
    planned_distance_miles,
    actual_delivery_time_hours,
    estimated_delivery_time_hours
FROM {{ ref('fact_shipments') }}
WHERE 
    -- Efficiency score should be between 0 and 100
    route_efficiency_score NOT BETWEEN 0 AND 100
    OR
    -- If actual equals planned, efficiency should be near 100
    (ABS(actual_distance_miles - planned_distance_miles) < 0.1 
     AND ABS(actual_delivery_time_hours - estimated_delivery_time_hours) < 0.1
     AND route_efficiency_score < 95)
    OR
    -- If actual is much worse than planned, efficiency should be low
    (actual_distance_miles > planned_distance_miles * 1.5
     AND route_efficiency_score > 50)