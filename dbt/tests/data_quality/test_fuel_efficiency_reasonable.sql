-- Test fuel efficiency reasonableness
-- Validates that fuel efficiency values are within realistic ranges

WITH fuel_efficiency_validation AS (
    SELECT 
        vehicle_id,
        vehicle_type,
        fuel_efficiency_mpg,
        model_year,
        current_mileage,
        
        -- Define reasonable ranges by vehicle type
        CASE 
            WHEN vehicle_type = 'TRUCK' THEN 
                CASE 
                    WHEN fuel_efficiency_mpg < 5 OR fuel_efficiency_mpg > 15 THEN 1 
                    ELSE 0 
                END
            WHEN vehicle_type = 'VAN' THEN 
                CASE 
                    WHEN fuel_efficiency_mpg < 8 OR fuel_efficiency_mpg > 20 THEN 1 
                    ELSE 0 
                END
            WHEN vehicle_type = 'MOTORCYCLE' THEN 
                CASE 
                    WHEN fuel_efficiency_mpg < 30 OR fuel_efficiency_mpg > 80 THEN 1 
                    ELSE 0 
                END
            WHEN vehicle_type = 'TRAILER' THEN 
                CASE 
                    WHEN fuel_efficiency_mpg < 3 OR fuel_efficiency_mpg > 12 THEN 1 
                    ELSE 0 
                END
            ELSE 
                CASE 
                    WHEN fuel_efficiency_mpg < 5 OR fuel_efficiency_mpg > 25 THEN 1 
                    ELSE 0 
                END
        END as efficiency_out_of_range,
        
        -- Check for vehicles with extremely low efficiency (possible data error)
        CASE 
            WHEN fuel_efficiency_mpg < 3 THEN 1 
            ELSE 0 
        END as extremely_low_efficiency,
        
        -- Check for vehicles with extremely high efficiency (possible data error)
        CASE 
            WHEN fuel_efficiency_mpg > 100 THEN 1 
            ELSE 0 
        END as extremely_high_efficiency,
        
        -- Check for missing efficiency data
        CASE 
            WHEN fuel_efficiency_mpg IS NULL THEN 1 
            ELSE 0 
        END as missing_efficiency
        
    FROM {{ ref('dim_vehicle') }}
    WHERE vehicle_status = 'ACTIVE'
)

SELECT 
    vehicle_id,
    vehicle_type,
    fuel_efficiency_mpg,
    model_year,
    current_mileage,
    efficiency_out_of_range,
    extremely_low_efficiency,
    extremely_high_efficiency,
    missing_efficiency
FROM fuel_efficiency_validation
WHERE 
    efficiency_out_of_range = 1
    OR extremely_low_efficiency = 1
    OR extremely_high_efficiency = 1
    OR missing_efficiency = 1
