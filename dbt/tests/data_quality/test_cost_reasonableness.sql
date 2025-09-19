-- Test that costs are reasonable and properly calculated
SELECT 
    shipment_id,
    fuel_cost_usd,
    driver_cost_usd, 
    total_cost_usd,
    revenue_usd,
    actual_distance_miles
FROM {{ ref('fact_shipments') }}
WHERE
    -- Total cost should equal sum of components
    ABS(total_cost_usd - (fuel_cost_usd + driver_cost_usd)) > 0.01
    OR
    -- Fuel cost per mile should be reasonable ($0.10 - $2.00 per mile)
    (fuel_cost_usd / NULLIF(actual_distance_miles, 0)) NOT BETWEEN 0.10 AND 2.00
    OR
    -- Revenue should generally be higher than cost (allowing for some loss-making shipments)
    revenue_usd < (total_cost_usd * 0.5)
    OR
    -- Driver cost should be reasonable ($15-50 per hour based on delivery time)
    (driver_cost_usd / NULLIF(actual_delivery_time_hours, 0)) NOT BETWEEN 15 AND 100