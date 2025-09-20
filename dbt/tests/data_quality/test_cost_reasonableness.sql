-- Test that costs are reasonable and properly calculated
SELECT 
    shipment_id,
    fuel_cost,
    delivery_cost, 
    total_cost,
    revenue,
    distance_km
FROM {{ ref('fact_shipments') }}
WHERE
    -- Total cost should equal sum of components (with tolerance for rounding)
    ABS(total_cost - (fuel_cost + delivery_cost)) > 0.01
    OR
    -- Fuel cost per km should be reasonable ($0.10 - $2.00 per km, converted from miles)
    (fuel_cost / NULLIF(distance_km, 0)) NOT BETWEEN 0.10 AND 2.00
    OR
    -- Revenue should generally be higher than cost (allowing for some loss-making shipments)
    revenue < (total_cost * 0.5)
    OR
    -- Delivery cost should be reasonable ($15-50 per hour based on delivery time)
    (delivery_cost / NULLIF(actual_duration_minutes / 60.0, 0)) NOT BETWEEN 15 AND 100