-- Test shipment status logic
-- Validates that shipment status transitions follow business rules

WITH shipment_status_validation AS (
    SELECT 
        shipment_id,
        customer_id,
        pickup_date,
        delivery_date,
        requested_delivery_date,
        actual_delivery_date,
        shipment_status,
        
        -- Calculate expected status based on dates
        CASE 
            WHEN pickup_date IS NULL THEN 'PENDING'
            WHEN pickup_date IS NOT NULL AND delivery_date IS NULL THEN 'IN_TRANSIT'
            WHEN delivery_date IS NOT NULL THEN 'DELIVERED'
            ELSE 'UNKNOWN'
        END as expected_status,
        
        -- Check for logical inconsistencies
        CASE 
            WHEN pickup_date > delivery_date THEN 1 
            ELSE 0 
        END as pickup_after_delivery,
        
        CASE 
            WHEN requested_delivery_date < pickup_date THEN 1 
            ELSE 0 
        END as delivery_before_pickup,
        
        CASE 
            WHEN actual_delivery_date IS NOT NULL AND delivery_date IS NULL THEN 1 
            ELSE 0 
        END as actual_without_delivery,
        
        -- Check for future dates
        CASE 
            WHEN pickup_date > CURRENT_DATE() THEN 1 
            ELSE 0 
        END as future_pickup,
        
        CASE 
            WHEN delivery_date > CURRENT_DATE() THEN 1 
            ELSE 0 
        END as future_delivery
        
    FROM {{ ref('fact_shipments') }}
    WHERE shipment_date >= DATEADD('day', -90, CURRENT_DATE())
)

SELECT 
    shipment_id,
    customer_id,
    shipment_status,
    expected_status,
    pickup_date,
    delivery_date,
    requested_delivery_date,
    actual_delivery_date,
    pickup_after_delivery,
    delivery_before_pickup,
    actual_without_delivery,
    future_pickup,
    future_delivery
FROM shipment_status_validation
WHERE 
    shipment_status != expected_status
    OR pickup_after_delivery = 1
    OR delivery_before_pickup = 1
    OR actual_without_delivery = 1
    OR future_pickup = 1
    OR future_delivery = 1
