-- Test foreign key constraints
-- Validates referential integrity across all fact and dimension tables

WITH foreign_key_validation AS (
    -- Test shipments -> customers
    SELECT 
        'shipments_to_customers' as constraint_name,
        s.shipment_id,
        s.customer_id,
        'Missing customer' as error_type
    FROM {{ ref('fact_shipments') }} s
    LEFT JOIN {{ ref('dim_customer') }} c ON s.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
    
    UNION ALL
    
    -- Test shipments -> vehicles
    SELECT 
        'shipments_to_vehicles' as constraint_name,
        s.shipment_id,
        s.vehicle_id,
        'Missing vehicle' as error_type
    FROM {{ ref('fact_shipments') }} s
    LEFT JOIN {{ ref('dim_vehicle') }} v ON s.vehicle_id = v.vehicle_id
    WHERE s.vehicle_id IS NOT NULL AND v.vehicle_id IS NULL
    
    UNION ALL
    
    -- Test shipments -> routes
    SELECT 
        'shipments_to_routes' as constraint_name,
        s.shipment_id,
        s.route_id,
        'Missing route' as error_type
    FROM {{ ref('fact_shipments') }} s
    LEFT JOIN {{ ref('dim_route') }} r ON s.route_id = r.route_id
    WHERE s.route_id IS NOT NULL AND r.route_id IS NULL
    
    UNION ALL
    
    -- Test routes -> locations (origin)
    SELECT 
        'routes_to_origin_locations' as constraint_name,
        r.route_id,
        r.origin_location_id,
        'Missing origin location' as error_type
    FROM {{ ref('dim_route') }} r
    LEFT JOIN {{ ref('dim_location') }} l ON r.origin_location_id = l.location_id
    WHERE l.location_id IS NULL
    
    UNION ALL
    
    -- Test routes -> locations (destination)
    SELECT 
        'routes_to_destination_locations' as constraint_name,
        r.route_id,
        r.destination_location_id,
        'Missing destination location' as error_type
    FROM {{ ref('dim_route') }} r
    LEFT JOIN {{ ref('dim_location') }} l ON r.destination_location_id = l.location_id
    WHERE l.location_id IS NULL
    
    UNION ALL
    
    -- Test shipments -> date dimension
    SELECT 
        'shipments_to_date' as constraint_name,
        s.shipment_id,
        s.shipment_date_id,
        'Missing date' as error_type
    FROM {{ ref('fact_shipments') }} s
    LEFT JOIN {{ ref('dim_date') }} d ON s.shipment_date_id = d.date_key
    WHERE s.shipment_date_id IS NOT NULL AND d.date_key IS NULL
)

SELECT 
    constraint_name,
    shipment_id,
    customer_id,
    error_type
FROM foreign_key_validation
ORDER BY constraint_name, shipment_id
