-- Test that delivery times are within reasonable bounds
SELECT 
    shipment_id,
    actual_duration_minutes,
    planned_duration_minutes,
    distance_km
FROM {{ ref('fact_shipments') }}
WHERE 
    -- Delivery time is unreasonably long (more than 7 days)
    actual_duration_minutes > (168 * 60)
    OR
    -- Delivery time is unreasonably short (less than distance/60 kmh converted to minutes)
    actual_duration_minutes < (distance_km / 60.0 * 60)
    OR  
    -- Actual time is more than 300% of estimated time
    actual_duration_minutes > (planned_duration_minutes * 3)