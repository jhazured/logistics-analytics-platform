-- Segments deliveries by distance/duration for different ML models
CREATE OR REPLACE VIEW ANALYTICS.view_haul_segmentation AS
WITH shipment_metrics AS (
    SELECT 
        s.shipment_id,
        s.customer_id,
        s.vehicle_id,
        s.route_id,
        s.shipment_date,
        s.delivery_status,
        s.revenue,
        s.total_cost,
        s.profit_margin_pct,
        
        -- Distance and duration metrics
        r.distance_km as route_distance_km,
        s.actual_duration_minutes / 60.0 as delivery_duration_hours,
        s.planned_duration_minutes / 60.0 as planned_duration_hours,
        
        -- Performance metrics
        CASE WHEN s.delivery_status = 'DELIVERED' AND s.actual_delivery_date <= s.planned_delivery_date THEN 1 ELSE 0 END as on_time_delivery,
        s.customer_rating,
        s.weather_delay_minutes,
        s.traffic_delay_minutes,
        
        -- Vehicle and route characteristics
        v.vehicle_type,
        v.capacity_kg,
        v.fuel_efficiency_mpg,
        r.route_type,
        
        -- Time-based features
        EXTRACT(HOUR FROM s.shipment_date) as shipment_hour,
        EXTRACT(DOW FROM s.shipment_date) as shipment_day_of_week,
        EXTRACT(MONTH FROM s.shipment_date) as shipment_month,
        
        -- Load characteristics
        s.weight_kg,
        s.volume_m3,
        s.priority_level,
        s.service_type
        
    FROM {{ ref('fact_shipments') }} s
    LEFT JOIN {{ ref('dim_route') }} r ON s.route_id = r.route_id
    LEFT JOIN {{ ref('dim_vehicle') }} v ON s.vehicle_id = v.vehicle_id
    WHERE s.shipment_date >= CURRENT_DATE() - 365
),
haul_segments AS (
    SELECT 
        shipment_id,
        customer_id,
        vehicle_id,
        route_id,
        shipment_date,
        delivery_status,
        revenue,
        total_cost,
        profit_margin_pct,
        route_distance_km,
        delivery_duration_hours,
        planned_duration_hours,
        on_time_delivery,
        customer_rating,
        weather_delay_minutes,
        traffic_delay_minutes,
        vehicle_type,
        capacity_kg,
        fuel_efficiency_mpg,
        route_type,
        shipment_hour,
        shipment_day_of_week,
        shipment_month,
        weight_kg,
        volume_m3,
        priority_level,
        service_type,
        
        -- Haul type segmentation
        CASE 
            WHEN route_distance_km <= 50 THEN 'short_haul'
            WHEN route_distance_km <= 200 THEN 'medium_haul' 
            ELSE 'long_haul'
        END AS haul_type,
        
        -- Delivery window segmentation
        CASE 
            WHEN delivery_duration_hours <= 4 THEN 'same_day'
            WHEN delivery_duration_hours <= 24 THEN 'next_day'
            ELSE 'multi_day'
        END AS delivery_window,
        
        -- Route complexity score (0-100)
        CASE 
            WHEN route_type = 'HIGHWAY' AND route_distance_km > 100 THEN 20
            WHEN route_type = 'HIGHWAY' AND route_distance_km <= 100 THEN 40
            WHEN route_type = 'ARTERIAL' AND route_distance_km > 50 THEN 60
            WHEN route_type = 'ARTERIAL' AND route_distance_km <= 50 THEN 80
            WHEN route_type = 'LOCAL' THEN 100
            ELSE 50
        END AS route_complexity_score,
        
        -- Traffic impact factor (0-100)
        CASE 
            WHEN traffic_delay_minutes > 60 THEN 100
            WHEN traffic_delay_minutes > 30 THEN 80
            WHEN traffic_delay_minutes > 15 THEN 60
            WHEN traffic_delay_minutes > 5 THEN 40
            WHEN traffic_delay_minutes > 0 THEN 20
            ELSE 0
        END AS traffic_impact_factor,
        
        -- Weather delay probability (0-100)
        CASE 
            WHEN weather_delay_minutes > 60 THEN 100
            WHEN weather_delay_minutes > 30 THEN 80
            WHEN weather_delay_minutes > 15 THEN 60
            WHEN weather_delay_minutes > 5 THEN 40
            WHEN weather_delay_minutes > 0 THEN 20
            ELSE 0
        END AS weather_delay_probability,
        
        -- Capacity utilization
        CASE 
            WHEN capacity_kg > 0 THEN (weight_kg / capacity_kg) * 100
            ELSE 0
        END AS capacity_utilization_pct,
        
        -- Efficiency metrics
        CASE 
            WHEN route_distance_km > 0 THEN (revenue / route_distance_km)
            ELSE 0
        END AS revenue_per_km,
        
        CASE 
            WHEN route_distance_km > 0 THEN (total_cost / route_distance_km)
            ELSE 0
        END AS cost_per_km,
        
        -- Time efficiency
        CASE 
            WHEN planned_duration_hours > 0 THEN (delivery_duration_hours / planned_duration_hours) * 100
            ELSE 100
        END AS time_efficiency_pct
        
    FROM shipment_metrics
)
SELECT 
    shipment_id,
    customer_id,
    vehicle_id,
    route_id,
    shipment_date,
    delivery_status,
    revenue,
    total_cost,
    profit_margin_pct,
    route_distance_km,
    delivery_duration_hours,
    planned_duration_hours,
    on_time_delivery,
    customer_rating,
    weather_delay_minutes,
    traffic_delay_minutes,
    vehicle_type,
    capacity_kg,
    fuel_efficiency_mpg,
    route_type,
    shipment_hour,
    shipment_day_of_week,
    shipment_month,
    weight_kg,
    volume_m3,
    priority_level,
    service_type,
    
    -- Segmentation results
    haul_type,
    delivery_window,
    route_complexity_score,
    traffic_impact_factor,
    weather_delay_probability,
    capacity_utilization_pct,
    revenue_per_km,
    cost_per_km,
    time_efficiency_pct,
    
    -- ML features for different haul types
    CASE 
        WHEN haul_type = 'short_haul' THEN route_complexity_score * 0.4 + traffic_impact_factor * 0.6
        WHEN haul_type = 'medium_haul' THEN route_complexity_score * 0.3 + traffic_impact_factor * 0.4 + weather_delay_probability * 0.3
        ELSE route_complexity_score * 0.2 + traffic_impact_factor * 0.3 + weather_delay_probability * 0.5
    END AS overall_difficulty_score,
    
    -- Performance indicators
    CASE 
        WHEN on_time_delivery = 1 AND customer_rating >= 4.0 THEN 'EXCELLENT'
        WHEN on_time_delivery = 1 AND customer_rating >= 3.0 THEN 'GOOD'
        WHEN on_time_delivery = 0 AND customer_rating >= 3.0 THEN 'FAIR'
        ELSE 'POOR'
    END AS performance_rating,
    
    -- Profitability indicators
    CASE 
        WHEN profit_margin_pct > 20 THEN 'HIGH_PROFIT'
        WHEN profit_margin_pct > 10 THEN 'MEDIUM_PROFIT'
        WHEN profit_margin_pct > 0 THEN 'LOW_PROFIT'
        ELSE 'LOSS'
    END AS profitability_rating,
    
    -- Risk indicators
    CASE 
        WHEN overall_difficulty_score > 80 OR profit_margin_pct < 0 THEN 'HIGH_RISK'
        WHEN overall_difficulty_score > 60 OR profit_margin_pct < 5 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS risk_level,
    
    CURRENT_TIMESTAMP() as segment_updated_at

FROM haul_segments
ORDER BY shipment_date DESC, revenue DESC