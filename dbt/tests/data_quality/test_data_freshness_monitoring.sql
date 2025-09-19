-- Test data freshness across all critical tables
WITH freshness_check AS (
    SELECT 
        'fact_shipments' as table_name,
        MAX(shipment_date) as last_data_date,
        CURRENT_DATE() as check_date,
        DATEDIFF('day', MAX(shipment_date), CURRENT_DATE()) as days_behind
    FROM {{ ref('fact_shipments') }}
    
    UNION ALL
    
    SELECT 
        'fact_vehicle_telemetry' as table_name,
        MAX(telemetry_date) as last_data_date,
        CURRENT_DATE() as check_date,
        DATEDIFF('day', MAX(telemetry_date), CURRENT_DATE()) as days_behind
    FROM {{ ref('fact_vehicle_telemetry') }}
)
SELECT 
    table_name,
    last_data_date,
    days_behind
FROM freshness_check
WHERE 
    -- Alert if data is more than 2 days behind for critical tables
    (table_name IN ('fact_shipments', 'fact_vehicle_telemetry') AND days_behind > 2)
    OR
    -- Alert if any table is more than 7 days behind
    days_behind > 7