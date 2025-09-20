-- Features for dynamic routing ML models
CREATE OR REPLACE VIEW ANALYTICS.view_route_optimization_features AS
WITH route_performance AS (
    SELECT 
        r.route_id,
        r.origin_location_id,
        r.destination_location_id,
        r.distance_km,
        r.route_type,
        
        -- Historical performance metrics (last 90 days)
        COUNT(s.shipment_id) as total_shipments_90d,
        AVG(s.actual_duration_minutes) as avg_delivery_time_minutes,
        AVG(CASE WHEN s.delivery_status = 'DELIVERED' THEN 1 ELSE 0 END) as delivery_success_rate,
        AVG(s.fuel_cost / NULLIF(r.distance_km, 0)) as avg_fuel_cost_per_km,
        AVG(s.route_efficiency_score) as avg_route_efficiency,
        AVG(s.customer_rating) as avg_customer_rating,
        AVG(s.weather_delay_minutes) as avg_weather_delay,
        AVG(s.traffic_delay_minutes) as avg_traffic_delay,
        
        -- Time-based patterns
        AVG(EXTRACT(HOUR FROM s.shipment_date)) as avg_hour_of_day,
        AVG(EXTRACT(DOW FROM s.shipment_date)) as avg_day_of_week,
        
        -- Seasonal patterns
        AVG(CASE WHEN EXTRACT(MONTH FROM s.shipment_date) IN (12,1,2) THEN 1 ELSE 0 END) as winter_usage_rate,
        AVG(CASE WHEN EXTRACT(MONTH FROM s.shipment_date) IN (6,7,8) THEN 1 ELSE 0 END) as summer_usage_rate,
        
        -- Performance trends
        AVG(CASE WHEN s.shipment_date >= CURRENT_DATE() - 30 THEN s.actual_duration_minutes END) as recent_avg_delivery_time,
        AVG(CASE WHEN s.shipment_date >= CURRENT_DATE() - 30 THEN s.route_efficiency_score END) as recent_route_efficiency
        
    FROM {{ ref('dim_route') }} r
    LEFT JOIN {{ ref('fact_shipments') }} s ON r.route_id = s.route_id
    WHERE s.shipment_date >= CURRENT_DATE() - 90
    GROUP BY r.route_id, r.origin_location_id, r.destination_location_id, r.distance_km, r.route_type
),
route_conditions AS (
    SELECT 
        rp.route_id,
        rp.origin_location_id,
        rp.destination_location_id,
        rp.distance_km,
        rp.route_type,
        rp.total_shipments_90d,
        rp.avg_delivery_time_minutes,
        rp.delivery_success_rate,
        rp.avg_fuel_cost_per_km,
        rp.avg_route_efficiency,
        rp.avg_customer_rating,
        rp.avg_weather_delay,
        rp.avg_traffic_delay,
        rp.avg_hour_of_day,
        rp.avg_day_of_week,
        rp.winter_usage_rate,
        rp.summer_usage_rate,
        rp.recent_avg_delivery_time,
        rp.recent_route_efficiency,
        
        -- Current time-based features
        EXTRACT(HOUR FROM CURRENT_TIMESTAMP()) as current_hour_of_day,
        EXTRACT(DOW FROM CURRENT_TIMESTAMP()) as current_day_of_week,
        EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) as current_month,
        
        -- Peak season indicator
        CASE 
            WHEN EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) IN (11,12,1,2) THEN true
            ELSE false
        END as is_peak_season,
        
        -- Current weather conditions (simplified)
        COALESCE(w.impact_score, 50) as current_weather_severity,
        
        -- Current traffic conditions (simplified)
        COALESCE(t.congestion_score, 50) as current_traffic_density_score,
        
        -- Road condition index (derived from route type and distance)
        CASE 
            WHEN rp.route_type = 'HIGHWAY' AND rp.distance_km > 100 THEN 90
            WHEN rp.route_type = 'HIGHWAY' AND rp.distance_km <= 100 THEN 80
            WHEN rp.route_type = 'ARTERIAL' THEN 70
            WHEN rp.route_type = 'LOCAL' THEN 60
            ELSE 50
        END as road_condition_index
        
    FROM route_performance rp
    LEFT JOIN {{ ref('dim_weather') }} w ON rp.origin_location_id = w.location_id 
        AND w.weather_date = CURRENT_DATE()
    LEFT JOIN {{ ref('dim_traffic_conditions') }} t ON rp.origin_location_id = t.location_id 
        AND t.traffic_date = CURRENT_DATE()
),
vehicle_suitability AS (
    SELECT 
        rc.route_id,
        rc.origin_location_id,
        rc.destination_location_id,
        rc.distance_km,
        rc.route_type,
        rc.total_shipments_90d,
        rc.avg_delivery_time_minutes,
        rc.delivery_success_rate,
        rc.avg_fuel_cost_per_km,
        rc.avg_route_efficiency,
        rc.avg_customer_rating,
        rc.avg_weather_delay,
        rc.avg_traffic_delay,
        rc.avg_hour_of_day,
        rc.avg_day_of_week,
        rc.winter_usage_rate,
        rc.summer_usage_rate,
        rc.recent_avg_delivery_time,
        rc.recent_route_efficiency,
        rc.current_hour_of_day,
        rc.current_day_of_week,
        rc.current_month,
        rc.is_peak_season,
        rc.current_weather_severity,
        rc.current_traffic_density_score,
        rc.road_condition_index,
        
        -- Vehicle suitability metrics
        AVG(v.capacity_kg) as avg_vehicle_capacity,
        AVG(v.fuel_efficiency_mpg) as avg_vehicle_fuel_efficiency,
        AVG(v.current_mileage) as avg_vehicle_mileage,
        AVG(CASE WHEN vm.maintenance_alert = true THEN 1 ELSE 0 END) as vehicle_maintenance_urgency,
        
        -- Driver experience (simplified - would come from driver dimension)
        AVG(CASE 
            WHEN v.current_mileage > 200000 THEN 5
            WHEN v.current_mileage > 150000 THEN 4
            WHEN v.current_mileage > 100000 THEN 3
            WHEN v.current_mileage > 50000 THEN 2
            ELSE 1
        END) as driver_experience_level
        
    FROM route_conditions rc
    LEFT JOIN {{ ref('fact_shipments') }} s ON rc.route_id = s.route_id
    LEFT JOIN {{ ref('dim_vehicle') }} v ON s.vehicle_id = v.vehicle_id
    LEFT JOIN {{ ref('fact_vehicle_telemetry') }} vm ON v.vehicle_id = vm.vehicle_id
    WHERE s.shipment_date >= CURRENT_DATE() - 30
    GROUP BY rc.route_id, rc.origin_location_id, rc.destination_location_id, rc.distance_km, rc.route_type,
             rc.total_shipments_90d, rc.avg_delivery_time_minutes, rc.delivery_success_rate, rc.avg_fuel_cost_per_km,
             rc.avg_route_efficiency, rc.avg_customer_rating, rc.avg_weather_delay, rc.avg_traffic_delay,
             rc.avg_hour_of_day, rc.avg_day_of_week, rc.winter_usage_rate, rc.summer_usage_rate,
             rc.recent_avg_delivery_time, rc.recent_route_efficiency, rc.current_hour_of_day, rc.current_day_of_week,
             rc.current_month, rc.is_peak_season, rc.current_weather_severity, rc.current_traffic_density_score,
             rc.road_condition_index
)
SELECT 
    route_id,
    origin_location_id,
    destination_location_id,
    distance_km,
    route_type,
    total_shipments_90d,
    avg_delivery_time_minutes,
    delivery_success_rate,
    avg_fuel_cost_per_km,
    avg_route_efficiency,
    avg_customer_rating,
    avg_weather_delay,
    avg_traffic_delay,
    avg_hour_of_day,
    avg_day_of_week,
    winter_usage_rate,
    summer_usage_rate,
    recent_avg_delivery_time,
    recent_route_efficiency,
    current_hour_of_day,
    current_day_of_week,
    current_month,
    is_peak_season,
    current_weather_severity,
    current_traffic_density_score,
    road_condition_index,
    avg_vehicle_capacity,
    avg_vehicle_fuel_efficiency,
    avg_vehicle_mileage,
    vehicle_maintenance_urgency,
    driver_experience_level,
    
    -- Haul type classification
    CASE 
        WHEN distance_km <= 50 THEN 'short_haul'
        WHEN distance_km <= 200 THEN 'medium_haul'
        ELSE 'long_haul'
    END as haul_type,
    
    -- Vehicle capacity utilization (simplified)
    CASE 
        WHEN avg_vehicle_capacity > 0 THEN (1000 / avg_vehicle_capacity) * 100  -- Assuming 1000kg average load
        ELSE 0
    END as vehicle_capacity_utilization,
    
    -- Route optimization score (0-100)
    CASE 
        WHEN delivery_success_rate > 0.95 AND avg_route_efficiency > 80 THEN 100
        WHEN delivery_success_rate > 0.85 AND avg_route_efficiency > 60 THEN 80
        WHEN delivery_success_rate > 0.70 AND avg_route_efficiency > 40 THEN 60
        WHEN delivery_success_rate > 0.50 THEN 40
        ELSE 20
    END as route_optimization_score,
    
    -- Real-time optimization factors
    (current_weather_severity + current_traffic_density_score + (100 - road_condition_index)) / 3.0 as current_difficulty_score,
    
    -- Performance indicators
    CASE 
        WHEN delivery_success_rate > 0.95 AND avg_customer_rating > 4.0 THEN 'EXCELLENT'
        WHEN delivery_success_rate > 0.85 AND avg_customer_rating > 3.5 THEN 'GOOD'
        WHEN delivery_success_rate > 0.70 AND avg_customer_rating > 3.0 THEN 'FAIR'
        ELSE 'POOR'
    END as route_performance_rating,
    
    -- Optimization recommendations
    CASE 
        WHEN current_difficulty_score > 80 THEN 'AVOID_ROUTE'
        WHEN current_difficulty_score > 60 THEN 'USE_ALTERNATIVE'
        WHEN current_difficulty_score > 40 THEN 'MONITOR_CONDITIONS'
        ELSE 'OPTIMAL_ROUTE'
    END as optimization_recommendation,
    
    CURRENT_TIMESTAMP() as features_updated_at

FROM vehicle_suitability
WHERE total_shipments_90d > 0
ORDER BY route_optimization_score DESC, total_shipments_90d DESC