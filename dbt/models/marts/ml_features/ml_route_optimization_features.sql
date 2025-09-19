-- 3. Route Optimization Features
-- File: models/analytics/ml_features/view_route_optimization_features.sql
{{ config(
    materialized='table',
    tags=['ml', 'features', 'optimization']
) }}

WITH route_performance_base AS (
    SELECT 
        fs.route_id,
        fs.vehicle_id,
        fs.shipment_date,
        fs.date_key,
        dr.route_name,
        dr.route_type,
        dr.total_distance_km,
        dr.estimated_duration_minutes,
        dr.complexity_score,
        dr.traffic_density,
        dr.weather_risk,
        
        -- Haul classification
        {{ classify_haul_type('fs.distance_km') }} AS haul_type,
        
        -- Temporal features
        dd.hour_of_day,
        dd.day_of_week,
        dd.day_name,
        dd.is_weekend,
        dd.is_holiday,
        dd.season,
        dd.logistics_day_type,
        
        -- Weather impact
        dw.condition AS weather_condition,
        dw.temperature_c,
        dw.precipitation_mm,
        dw.wind_speed_kmh,
        dw.weather_severity_score,
        dw.driving_impact_score,
        
        -- Vehicle characteristics
        dv.vehicle_type,
        dv.capacity_kg,
        dv.fuel_efficiency_l_100km,
        dv.condition_score AS vehicle_condition,
        
        -- Performance metrics
        fs.planned_duration_minutes,
        fs.actual_duration_minutes,
        fs.is_on_time,
        fs.fuel_cost,
        fs.delivery_cost,
        fs.customer_rating,
        
        -- Load characteristics
        fs.weight_kg,
        fs.volume_m3,
        fs.weight_kg / NULLIF(dv.capacity_kg, 0) AS weight_utilization,
        
    FROM {{ ref('fact_shipments') }} fs
    JOIN {{ ref('dim_route') }} dr ON fs.route_id = dr.route_id
    JOIN {{ ref('dim_date') }} dd ON fs.date_key = dd.date_key
    JOIN {{ ref('dim_vehicle') }} dv ON fs.vehicle_id = dv.vehicle_id
    LEFT JOIN {{ ref('dim_weather') }} dw 
        ON dd.date = dw.date 
        AND dv.vehicle_id IN (
            SELECT DISTINCT vehicle_id 
            FROM {{ ref('fact_shipments') }} 
            WHERE route_id = fs.route_id
        )
    WHERE fs.is_delivered = TRUE
        AND fs.shipment_date >= CURRENT_DATE() - 365
),

route_historical_performance AS (
    SELECT 
        route_id,
        haul_type,
        
        -- Overall performance metrics
        COUNT(*) AS historical_trips,
        AVG(actual_duration_minutes) AS avg_actual_duration,
        AVG(planned_duration_minutes) AS avg_planned_duration,
        AVG(actual_duration_minutes / NULLIF(planned_duration_minutes, 1)) AS avg_duration_ratio,
        
        -- Reliability metrics
        AVG(CASE WHEN is_on_time THEN 1.0 ELSE 0.0 END) AS historical_on_time_rate,
        STDDEV(actual_duration_minutes) AS duration_variability,
        STDDEV(fuel_cost) AS fuel_cost_variability,
        AVG(customer_rating) AS avg_customer_satisfaction,
        
        -- Cost metrics
        AVG(fuel_cost) AS avg_fuel_cost,
        AVG(delivery_cost) AS avg_delivery_cost,
        AVG(fuel_cost + delivery_cost) AS avg_total_cost,
        
        -- Efficiency metrics
        AVG(total_distance_km / NULLIF(actual_duration_minutes, 0) * 60) AS avg_speed_kmh,
        AVG(weight_utilization) AS avg_capacity_utilization,
        
        -- Weather sensitivity
        AVG(CASE WHEN weather_condition IN ('Heavy Rain', 'Storm', 'Fog') 
                 THEN actual_duration_minutes 
                 ELSE NULL END) AS avg_duration_bad_weather,
        AVG(CASE WHEN weather_condition IN ('Clear', 'Partly Cloudy') 
                 THEN actual_duration_minutes 
                 ELSE NULL END) AS avg_duration_good_weather,
                 
        -- Temporal patterns
        AVG(CASE WHEN is_weekend THEN actual_duration_minutes ELSE NULL END) AS avg_weekend_duration,
        AVG(CASE WHEN NOT is_weekend THEN actual_duration_minutes ELSE NULL END) AS avg_weekday_duration,
        AVG(CASE WHEN hour_of_day BETWEEN 7 AND 9 THEN actual_duration_minutes ELSE NULL END) AS avg_morning_peak_duration,
        AVG(CASE WHEN hour_of_day BETWEEN 17 AND 19 THEN actual_duration_minutes ELSE NULL END) AS avg_evening_peak_duration,
        
        -- Seasonal variations
        AVG(CASE WHEN season = 'Summer' THEN actual_duration_minutes ELSE NULL END) AS avg_summer_duration,
        AVG(CASE WHEN season = 'Winter' THEN actual_duration_minutes ELSE NULL END) AS avg_winter_duration
        
    FROM route_performance_base
    GROUP BY 1, 2
),

current_conditions AS (
    SELECT DISTINCT
        rpb.route_id,
        rpb.haul_type,
        rpb.shipment_date,
        rpb.hour_of_day,
        rpb.day_of_week,
        rpb.is_weekend,
        rpb.is_holiday,
        rpb.season,
        rpb.logistics_day_type,
        
        -- Real-time weather conditions
        rpb.weather_condition,
        rpb.temperature_c,
        rpb.precipitation_mm,
        rpb.wind_speed_kmh,
        rpb.weather_severity_score,
        rpb.driving_impact_score,
        
        -- Route characteristics
        rpb.route_type,
        rpb.total_distance_km,
        rpb.estimated_duration_minutes,
        rpb.complexity_score,
        rpb.traffic_density,
        rpb.weather_risk,
        
        -- Expected conditions impact
        CASE 
            WHEN rpb.weather_condition IN ('Heavy Rain', 'Storm', 'Fog') THEN 1.3
            WHEN rpb.weather_condition IN ('Light Rain', 'Cloudy') THEN 1.1
            ELSE 1.0
        END AS weather_delay_factor,
        
        CASE 
            WHEN rpb.is_weekend THEN 0.9
            WHEN rpb.hour_of_day BETWEEN 7 AND 9 OR rpb.hour_of_day BETWEEN 17 AND 19 THEN 1.2
            WHEN rpb.is_holiday THEN 0.8
            ELSE 1.0
        END AS time_delay_factor,
        
        CASE 
            WHEN rpb.traffic_density = 'High' THEN 1.25
            WHEN rpb.traffic_density = 'Medium' THEN 1.1
            ELSE 1.0
        END AS traffic_delay_factor
        
    FROM route_performance_base rpb
    WHERE rpb.shipment_date = CURRENT_DATE()
)

SELECT 
    cc.route_id,
    cc.haul_type,
    cc.shipment_date,
    
    -- Temporal features
    cc.hour_of_day,
    cc.day_of_week,
    cc.is_weekend,
    cc.is_holiday,
    cc.season,
    cc.logistics_day_type,
    
    -- Weather features
    cc.weather_condition,
    cc.temperature_c,
    cc.precipitation_mm,
    cc.wind_speed_kmh,
    cc.weather_severity_score,
    cc.driving_impact_score,
    
    -- Route characteristics
    cc.route_type,
    cc.total_distance_km,
    cc.estimated_duration_minutes,
    cc.complexity_score,
    cc.traffic_density,
    cc.weather_risk,
    
    -- Historical performance features
    rhp.historical_trips,
    rhp.avg_actual_duration,
    rhp.avg_planned_duration,
    rhp.avg_duration_ratio,
    rhp.historical_on_time_rate,
    rhp.duration_variability,
    rhp.avg_customer_satisfaction,
    rhp.avg_fuel_cost,
    rhp.avg_delivery_cost,
    rhp.avg_total_cost,
    rhp.avg_speed_kmh,
    rhp.avg_capacity_utilization,
    
    -- Weather sensitivity features
    rhp.avg_duration_bad_weather,
    rhp.avg_duration_good_weather,
    COALESCE(rhp.avg_duration_bad_weather / NULLIF(rhp.avg_duration_good_weather, 0), 1) AS weather_sensitivity_ratio,
    
    -- Temporal pattern features
    rhp.avg_weekend_duration,
    rhp.avg_weekday_duration,
    rhp.avg_morning_peak_duration,
    rhp.avg_evening_peak_duration,
    
    -- Seasonal features
    rhp.avg_summer_duration,
    rhp.avg_winter_duration,
    
    -- Delay factor features
    cc.weather_delay_factor,
    cc.time_delay_factor,
    cc.traffic_delay_factor,
    cc.weather_delay_factor * cc.time_delay_factor * cc.traffic_delay_factor AS combined_delay_factor,
    
    -- Predicted performance
    cc.estimated_duration_minutes * cc.weather_delay_factor * cc.time_delay_factor * cc.traffic_delay_factor AS predicted_duration_minutes,
    
    -- Risk scores
    CASE 
        WHEN rhp.duration_variability > rhp.avg_actual_duration * 0.3 THEN 'high_risk'
        WHEN rhp.duration_variability > rhp.avg_actual_duration * 0.15 THEN 'medium_risk'
        ELSE 'low_risk'
    END AS variability_risk,
    
    CASE 
        WHEN rhp.historical_on_time_rate < 0.7 THEN 'high_risk'
        WHEN rhp.historical_on_time_rate < 0.85 THEN 'medium_risk'
        ELSE 'low_risk'
    END AS on_time_risk,
    
    -- Optimization recommendations
    CASE 
        WHEN cc.weather_delay_factor > 1.2 AND rhp.historical_on_time_rate < 0.8 
        THEN 'consider_delay_or_reroute'
        WHEN cc.combined_delay_factor > 1.3 
        THEN 'high_delay_expected'
        WHEN rhp.avg_capacity_utilization < 0.6 
        THEN 'consolidation_opportunity'
        ELSE 'proceed_as_planned'
    END AS route_recommendation

FROM current_conditions cc
LEFT JOIN route_historical_performance rhp 
    ON cc.route_id = rhp.route_id 
    AND cc.haul_type = rhp.haul_type