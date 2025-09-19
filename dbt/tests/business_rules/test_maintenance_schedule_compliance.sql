-- Test that vehicles are following maintenance schedules
WITH vehicle_maintenance AS (
    SELECT 
        v.vehicle_id,
        v.vehicle_number,
        v.current_mileage,
        v.maintenance_interval_miles,
        vm.last_maintenance_date,
        vm.last_maintenance_mileage,
        v.current_mileage - vm.last_maintenance_mileage as miles_since_maintenance
    FROM {{ ref('dim_vehicle') }} v
    LEFT JOIN {{ ref('dim_vehicle_maintenance') }} vm 
        ON v.vehicle_id = vm.vehicle_id
    WHERE v.vehicle_status = 'ACTIVE'
)
SELECT 
    vehicle_id,
    vehicle_number,
    miles_since_maintenance,
    maintenance_interval_miles,
    last_maintenance_date
FROM vehicle_maintenance
WHERE 
    -- Overdue for maintenance (exceeded interval by more than 20%)
    miles_since_maintenance > (maintenance_interval_miles * 1.2)
    OR
    -- No maintenance record for active vehicle with significant mileage
    (last_maintenance_date IS NULL AND current_mileage > 10000)