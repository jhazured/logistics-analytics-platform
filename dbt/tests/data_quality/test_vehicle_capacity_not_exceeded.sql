-- Test that vehicle capacity limits are respected
SELECT 
    s.shipment_id,
    s.weight_kg as shipment_weight,
    v.capacity_kg as vehicle_capacity,
    (s.weight_kg / v.capacity_kg) * 100 as capacity_utilization_pct
FROM {{ ref('fact_shipments') }} s
JOIN {{ ref('dim_vehicle') }} v ON s.vehicle_id = v.vehicle_id
WHERE 
    s.weight_kg > v.capacity_kg  -- Exceeded capacity
    OR
    (s.weight_kg / v.capacity_kg) > 1.05  -- Allow 5% tolerance for measurement variance
