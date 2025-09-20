-- Test fuel efficiency reasonableness
-- Validates that fuel efficiency values are within realistic ranges

WITH fuel_efficiency_validation AS (
    SELECT 
        vehicle_id,
        vehicle_type,
        fuel_efficiency_mpg,
        model_year,
        current_mileage,
        
        -- Define reasonable ranges by vehicle type with better validation
        CASE 
            WHEN fuel_efficiency_mpg IS NULL THEN 'missing'
            WHEN fuel_efficiency_mpg <= 0 THEN 'invalid_zero_negative'
            WHEN vehicle_type = 'TRUCK' THEN 
                CASE 
                    WHEN fuel_efficiency_mpg < 4 THEN 'too_low'
                    WHEN fuel_efficiency_mpg > 18 THEN 'too_high'
                    ELSE 'valid'
                END
            WHEN vehicle_type = 'VAN' THEN 
                CASE 
                    WHEN fuel_efficiency_mpg < 6 THEN 'too_low'
                    WHEN fuel_efficiency_mpg > 25 THEN 'too_high'
                    ELSE 'valid'
                END
            WHEN vehicle_type = 'MOTORCYCLE' THEN 
                CASE 
                    WHEN fuel_efficiency_mpg < 25 THEN 'too_low'
                    WHEN fuel_efficiency_mpg > 90 THEN 'too_high'
                    ELSE 'valid'
                END
            WHEN vehicle_type = 'TRAILER' THEN 
                CASE 
                    WHEN fuel_efficiency_mpg < 2 THEN 'too_low'
                    WHEN fuel_efficiency_mpg > 15 THEN 'too_high'
                    ELSE 'valid'
                END
            ELSE 
                CASE 
                    WHEN fuel_efficiency_mpg < 4 THEN 'too_low'
                    WHEN fuel_efficiency_mpg > 30 THEN 'too_high'
                    ELSE 'valid'
                END
        END as validation_result,
        
        -- Additional validation flags
        CASE 
            WHEN fuel_efficiency_mpg < 1 THEN 1 
            ELSE 0 
        END as extremely_low_efficiency,
        
        CASE 
            WHEN fuel_efficiency_mpg > 100 THEN 1 
            ELSE 0 
        END as extremely_high_efficiency,
        
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
    validation_result,
    extremely_low_efficiency,
    extremely_high_efficiency,
    missing_efficiency
FROM fuel_efficiency_validation
WHERE 
    validation_result != 'valid'
    OR extremely_low_efficiency = 1
    OR extremely_high_efficiency = 1
    OR missing_efficiency = 1
