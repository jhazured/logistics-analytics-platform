-- Test maintenance interval compliance
-- Validates that vehicles are maintained according to schedule

WITH maintenance_compliance AS (
    SELECT 
        v.vehicle_id,
        v.vehicle_type,
        v.current_mileage,
        v.maintenance_interval_miles,
        v.last_maintenance_mileage,
        v.next_maintenance_due_mileage,
        
        -- Calculate miles since last maintenance
        v.current_mileage - COALESCE(v.last_maintenance_mileage, 0) as miles_since_maintenance,
        
        -- Calculate expected next maintenance
        COALESCE(v.last_maintenance_mileage, 0) + v.maintenance_interval_miles as expected_next_maintenance,
        
        -- Check if maintenance is overdue
        CASE 
            WHEN v.current_mileage > COALESCE(v.next_maintenance_due_mileage, 0) THEN 1 
            ELSE 0 
        END as is_overdue,
        
        -- Check if maintenance is due soon (within 1000 miles)
        CASE 
            WHEN v.current_mileage >= COALESCE(v.next_maintenance_due_mileage, 0) - 1000 THEN 1 
            ELSE 0 
        END as is_due_soon
        
    FROM {{ ref('dim_vehicle') }} v
    WHERE v.vehicle_status = 'ACTIVE'
),

maintenance_anomalies AS (
    SELECT 
        vehicle_id,
        vehicle_type,
        current_mileage,
        miles_since_maintenance,
        expected_next_maintenance,
        next_maintenance_due_mileage,
        is_overdue,
        is_due_soon,
        
        -- Flag vehicles with maintenance intervals that seem too long or too short
        CASE 
            WHEN maintenance_interval_miles > 25000 THEN 'INTERVAL_TOO_LONG'
            WHEN maintenance_interval_miles < 3000 THEN 'INTERVAL_TOO_SHORT'
            WHEN miles_since_maintenance > maintenance_interval_miles * 1.5 THEN 'SEVERELY_OVERDUE'
            WHEN is_overdue = 1 THEN 'OVERDUE'
            ELSE 'OK'
        END as maintenance_status
        
    FROM maintenance_compliance
)

SELECT 
    vehicle_id,
    vehicle_type,
    current_mileage,
    miles_since_maintenance,
    expected_next_maintenance,
    next_maintenance_due_mileage,
    maintenance_status
FROM maintenance_anomalies
WHERE maintenance_status != 'OK'
