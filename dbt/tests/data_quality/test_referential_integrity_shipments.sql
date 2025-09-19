-- Comprehensive referential integrity test for shipments
SELECT 
    'Missing Customer' as error_type,
    s.shipment_id,
    s.customer_id,
    NULL as vehicle_id,
    NULL as route_id
FROM {{ ref('fact_shipments') }} s
LEFT JOIN {{ ref('dim_customer') }} c ON s.customer_id = c.customer_id
WHERE c.customer_id IS NULL

UNION ALL

SELECT 
    'Missing Vehicle' as error_type,
    s.shipment_id,
    NULL as customer_id, 
    s.vehicle_id,
    NULL as route_id
FROM {{ ref('fact_shipments') }} s
LEFT JOIN {{ ref('dim_vehicle') }} v ON s.vehicle_id = v.vehicle_id  
WHERE s.vehicle_id IS NOT NULL AND v.vehicle_id IS NULL

UNION ALL

SELECT 
    'Missing Route' as error_type,
    s.shipment_id,
    NULL as customer_id,
    NULL as vehicle_id,
    s.route_id
FROM {{ ref('fact_shipments') }} s
LEFT JOIN {{ ref('dim_route') }} r ON s.route_id = r.route_id
WHERE s.route_id IS NOT NULL AND r.route_id IS NULL