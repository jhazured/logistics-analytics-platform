-- Test maintenance interval compliance
-- Validates that vehicles are maintained according to schedule

WITH maintenance_compliance AS (
    SELECT 
        v.vehicle_id,
        v.vehicle_type,
        v.current_mileage,
        v.maintenance_interval_miles,
        v.last_maintenance_date,
        v.next_maintenance_date,
        
        -- Calculate miles since last maintenance (using current mileage as proxy)
        v.current_mileage as miles_since_maintenance,
        
        -- Calculate expected next maintenance (simplified - using interval)
        v.current_mileage + v.maintenance_interval_miles as expected_next_maintenance,
        
        -- Check if maintenance is overdue (simplified - check if next maintenance date has passed)
        CASE 
            WHEN v.next_maintenance_date IS NOT NULL AND v.next_maintenance_date < CURRENT_DATE() THEN 1 
            ELSE 0 
        END as is_overdue,
        
        -- Check if maintenance is due soon (within 30 days)
        CASE 
            WHEN v.next_maintenance_date IS NOT NULL AND v.next_maintenance_date <= DATEADD('day', 30, CURRENT_DATE()) THEN 1 
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
        next_maintenance_date,
        is_overdue,
        is_due_soon,
        
        -- Flag vehicles with maintenance intervals that seem too long or too short
        CASE 
            WHEN maintenance_interval_miles > 25000 THEN 'INTERVAL_TOO_LONG'
            WHEN maintenance_interval_miles < 3000 THEN 'INTERVAL_TOO_SHORT'
            WHEN is_overdue = 1 THEN 'OVERDUE'
            WHEN is_due_soon = 1 THEN 'DUE_SOON'
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
    next_maintenance_date,
    maintenance_status
FROM maintenance_anomalies
WHERE maintenance_status != 'OK'
