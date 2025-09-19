-- Test that delivery times are within reasonable bounds
SELECT 
    shipment_id,
    actual_delivery_time_hours,
    estimated_delivery_time_hours,
    actual_distance_miles
FROM {{ ref('fact_shipments') }}
WHERE 
    -- Delivery time is unreasonably long (more than 7 days)
    actual_delivery_time_hours > 168
    OR
    -- Delivery time is unreasonably short (less than distance/60 mph)
    actual_delivery_time_hours < (actual_distance_miles / 60.0)
    OR  
    -- Actual time is more than 300% of estimated time
    actual_delivery_time_hours > (estimated_delivery_time_hours * 3)