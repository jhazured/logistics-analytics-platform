-- 22. Sustainability Metrics
-- File: models/analytics/monitoring/view_sustainability_metrics.sql
{{ config(
    materialized='view',
    tags=['monitoring', 'sustainability', 'esg']
) }}

WITH vehicle_emissions AS (
    SELECT 
        fs.shipment_date,
        dv.vehicle_id,
        dv.vehicle_type,
        dv.fuel_type,
        dv.year AS vehicle_year,
        dl_origin.city AS origin_city,
        dl_origin.state AS origin_state,
        
        -- Distance and fuel consumption
        SUM(fs.distance_km) AS total_distance_km,
        SUM(fs.fuel_cost) AS total_fuel_cost,
        AVG(dv.fuel_efficiency_l_100km) AS avg_fuel_efficiency,
        
        -- Calculate fuel consumption (liters)
        SUM(fs.distance_km) * AVG(dv.fuel_efficiency_l_100km) / 100 AS estimated_fuel_liters,
        
        -- CO2 emissions calculation (kg CO2 per liter)
        -- Diesel: 2.68 kg CO2/L, Petrol: 2.31 kg CO2/L, Electric: 0 direct emissions
        SUM(fs.distance_km) * AVG(dv.fuel_efficiency_l_100km) / 100 * 
        CASE 
            WHEN dv.fuel_type = 'Diesel' THEN 2.68
            WHEN dv.fuel_type = 'Petrol' THEN 2.31
            WHEN dv.fuel_type = 'Electric' THEN 0
            WHEN dv.fuel_type = 'Hybrid' THEN 1.5  -- Estimated average
            ELSE 2.5  -- Default assumption
        END AS daily_co2_emissions_kg,
        
        COUNT(*) AS daily_deliveries
        
    FROM {{ ref('fact_shipments') }} fs
    JOIN {{ ref('dim_vehicle') }} dv ON fs.vehicle_id = dv.vehicle_id
    JOIN {{ ref('dim_location') }} dl_origin ON fs.origin_location_id = dl_origin.location_id
    WHERE fs.shipment_date >= CURRENT_DATE() - 365
        AND fs.is_delivered = TRUE
    GROUP BY 1,2,3,4,5,6,7
),

fleet_sustainability_metrics AS (
    SELECT 
        shipment_date,
        origin_city,
        origin_state,
        
        -- Fleet composition
        COUNT(DISTINCT vehicle_id) AS active_vehicles,
        COUNT(DISTINCT CASE WHEN fuel_type = 'Electric' THEN vehicle_id END) AS electric_vehicles,
        COUNT(DISTINCT CASE WHEN fuel_type = 'Hybrid' THEN vehicle_id END) AS hybrid_vehicles,
        COUNT(DISTINCT CASE WHEN fuel_type IN ('Diesel', 'Petrol') THEN vehicle_id END) AS traditional_vehicles,
        
        -- Environmental impact
        SUM(total_distance_km) AS total_distance_km,
        SUM(estimated_fuel_liters) AS total_fuel_consumption_liters,
        SUM(daily_co2_emissions_kg) AS total_co2_emissions_kg,
        SUM(daily_deliveries) AS total_deliveries,
        
        -- Efficiency metrics
        AVG(avg_fuel_efficiency) AS fleet_avg_fuel_efficiency,
        SUM(daily_co2_emissions_kg) / NULLIF(SUM(total_distance_km), 0) AS co2_per_km,
        SUM(daily_co2_emissions_kg) / NULLIF(SUM(daily_deliveries), 0) AS co2_per_delivery,
        
        -- Vehicle age impact
        AVG(2024 - vehicle_year) AS avg_vehicle_age,
        COUNT(DISTINCT CASE WHEN vehicle_year >= 2020 THEN vehicle_id END) AS modern_vehicles,
        COUNT(DISTINCT CASE WHEN vehicle_year < 2015 THEN vehicle_id END) AS older_vehicles
        
    FROM vehicle_emissions
    GROUP BY 1,2,3
)

SELECT 
    shipment_date,
    origin_city,
    origin_state,
    
    -- Fleet composition metrics
    active_vehicles,
    electric_vehicles,
    hybrid_vehicles,
    traditional_vehicles,
    ROUND(electric_vehicles::FLOAT / NULLIF(active_vehicles, 0) * 100, 1) AS electric_vehicle_percentage,
    ROUND((electric_vehicles + hybrid_vehicles)::FLOAT / NULLIF(active_vehicles, 0) * 100, 1) AS eco_friendly_percentage,
    
    -- Environmental impact
    ROUND(total_distance_km, 1) AS total_distance_km,
    ROUND(total_fuel_consumption_liters, 1) AS fuel_consumption_liters,
    ROUND(total_co2_emissions_kg, 1) AS co2_emissions_kg,
    ROUND(total_co2_emissions_kg / 1000, 2) AS co2_emissions_tonnes,
    total_deliveries,
    
    -- Efficiency and intensity metrics
    ROUND(fleet_avg_fuel_efficiency, 2) AS avg_fuel_efficiency_l_per_100km,
    ROUND(co2_per_km * 1000, 2) AS co2_grams_per_km,
    ROUND(co2_per_delivery, 2) AS co2_kg_per_delivery,
    
    -- Fleet modernization metrics
    ROUND(avg_vehicle_age, 1) AS avg_vehicle_age_years,
    modern_vehicles,
    older_vehicles,
    ROUND(modern_vehicles::FLOAT / NULLIF(active_vehicles, 0) * 100, 1) AS modern_fleet_percentage,
    
    -- Sustainability scoring (0-100)
    GREATEST(0, LEAST(100,
        (electric_vehicle_percentage * 0.4) +
        (eco_friendly_percentage * 0.3) +
        (CASE WHEN co2_per_km < 0.5 THEN 20 
              WHEN co2_per_km < 0.8 THEN 15 
              WHEN co2_per_km < 1.2 THEN 10 
              ELSE 5 END) +
        (modern_fleet_percentage * 0.1)
    )) AS sustainability_score,
    
    -- ESG reporting categories
    CASE 
        WHEN electric_vehicle_percentage >= 50 THEN 'leading'
        WHEN electric_vehicle_percentage >= 25 THEN 'advanced'
        WHEN electric_vehicle_percentage >= 10 THEN 'developing'
        ELSE 'emerging'
    END AS electrification_maturity,
    
    -- Carbon reduction targets (example: 20% reduction year-over-year)
    LAG(total_co2_emissions_kg, 365) OVER (
        PARTITION BY origin_city 
        ORDER BY shipment_date
    ) AS co2_emissions_same_date_last_year,
    
    ROUND((total_co2_emissions_kg - LAG(total_co2_emissions_kg, 365) OVER (
        PARTITION BY origin_city ORDER BY shipment_date
    )) / NULLIF(LAG(total_co2_emissions_kg, 365) OVER (
        PARTITION BY origin_city ORDER BY shipment_date
    ), 0) * 100, 1) AS co2_yoy_change_percent,
    
    -- Target achievement
    CASE 
        WHEN co2_yoy_change_percent <= -20 THEN 'exceeding_target'
        WHEN co2_yoy_change_percent <= -10 THEN 'on_target'
        WHEN co2_yoy_change_percent <= 0 THEN 'below_target'
        ELSE 'increasing_emissions'
    END AS carbon_target_status

FROM fleet_sustainability_metrics