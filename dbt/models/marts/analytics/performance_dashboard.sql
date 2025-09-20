-- =====================================================
-- Business Intelligence Views
-- =====================================================

-- Performance Dashboard
{{ config(
    materialized='view',
    tags=['bi', 'dashboard', 'performance']
) }}

WITH current_period AS (
    SELECT 
        fs.shipment_date,
        dl_origin.city AS origin_city,
        dl_origin.state AS origin_state,
        dc.volume_segment,
        dc.customer_type,
        dr.route_type,
        dv.vehicle_type,
        dd.season,
        dd.is_weekend,
        
        -- Core KPIs
        COUNT(*) AS delivery_count,
        COUNT(DISTINCT fs.customer_id) AS unique_customers,
        COUNT(DISTINCT fs.vehicle_id) AS active_vehicles,
        
        -- Performance metrics
        AVG(CASE WHEN fs.is_on_time THEN 1.0 ELSE 0.0 END) AS on_time_rate,
        AVG(fs.route_efficiency_score) AS avg_customer_satisfaction,
        AVG(fs.actual_duration_minutes / NULLIF(fs.planned_duration_minutes, 1)) AS schedule_adherence,
        
        -- Financial metrics
        SUM(fs.revenue) AS total_revenue,
        SUM(fs.delivery_cost) AS total_delivery_cost,
        SUM(fs.fuel_cost) AS total_fuel_cost,
        SUM(fs.revenue - fs.delivery_cost - fs.fuel_cost) AS total_profit,
        AVG(fs.revenue) AS avg_revenue_per_delivery,
        
        -- Efficiency metrics
        SUM(fs.distance_km) AS total_distance,
        AVG(fs.weight_kg / NULLIF(dv.capacity_kg, 1)) AS avg_capacity_utilization,
        SUM(fs.distance_km) / NULLIF(SUM(fs.actual_duration_minutes), 0) * 60 AS avg_speed_kmh,
        SUM(fs.fuel_cost) / NULLIF(SUM(fs.distance_km), 0) AS fuel_cost_per_km,
        
        -- Volume metrics
        SUM(fs.weight_kg) AS total_weight_kg,
        SUM(fs.volume_m3) AS total_volume_m3,
        AVG(fs.weight_kg) AS avg_shipment_weight,
        
        -- Service level metrics
        COUNT(CASE WHEN fs.priority_level = 'Urgent' THEN 1 END) AS urgent_deliveries,
        COUNT(CASE WHEN fs.service_type = 'Express' THEN 1 END) AS express_deliveries,
        AVG(CASE WHEN fs.priority_level = 'Urgent' AND fs.is_on_time THEN 1.0 ELSE 0.0 END) AS urgent_on_time_rate
        
    FROM {{ ref('fact_shipments') }} fs
    JOIN {{ ref('dim_location') }} dl_origin ON fs.origin_location_id = dl_origin.location_id
    JOIN {{ ref('dim_customer') }} dc ON fs.customer_id = dc.customer_id
    JOIN {{ ref('dim_route') }} dr ON fs.route_id = dr.route_id
    JOIN {{ ref('dim_vehicle') }} dv ON fs.vehicle_id = dv.vehicle_id
    JOIN {{ ref('dim_date') }} dd ON to_date(fs.shipment_date) = dd.date
    WHERE fs.shipment_date >= CURRENT_DATE() - 90  -- Last 90 days
    GROUP BY 1,2,3,4,5,6,7,8,9
)

SELECT 
    shipment_date,
    origin_city,
    origin_state,
    volume_segment,
    customer_type,
    route_type,
    vehicle_type,
    season,
    is_weekend,
    
    -- Volume KPIs
    delivery_count,
    unique_customers,
    active_vehicles,
    total_weight_kg,
    total_volume_m3,
    avg_shipment_weight,
    
    -- Performance KPIs
    ROUND(on_time_rate * 100, 1) AS on_time_percentage,
    ROUND(avg_customer_satisfaction, 2) AS satisfaction_score,
    ROUND(schedule_adherence * 100, 1) AS schedule_adherence_percentage,
    
    -- Financial KPIs
    ROUND(total_revenue, 2) AS revenue,
    ROUND(total_delivery_cost, 2) AS delivery_costs,
    ROUND(total_fuel_cost, 2) AS fuel_costs,
    ROUND(total_profit, 2) AS profit,
    ROUND(total_profit / NULLIF(total_revenue, 0) * 100, 1) AS profit_margin_percentage,
    ROUND(avg_revenue_per_delivery, 2) AS revenue_per_delivery,
    
    -- Efficiency KPIs
    ROUND(total_distance, 1) AS total_distance_km,
    ROUND(avg_capacity_utilization * 100, 1) AS capacity_utilization_percentage,
    ROUND(avg_speed_kmh, 1) AS average_speed_kmh,
    ROUND(fuel_cost_per_km, 4) AS fuel_cost_per_km,
    
    -- Service Level KPIs
    urgent_deliveries,
    express_deliveries,
    ROUND(urgent_on_time_rate * 100, 1) AS urgent_on_time_percentage,
    
    -- Calculated performance scores
    ROUND(
        (on_time_rate * 0.4) + 
        (avg_capacity_utilization * 0.3) + 
        (avg_customer_satisfaction / 10.0 * 0.3), 2
    ) AS overall_performance_score,
    
    -- Productivity metrics
    ROUND(delivery_count / NULLIF(active_vehicles, 0), 1) AS deliveries_per_vehicle,
    ROUND(total_revenue / NULLIF(active_vehicles, 0), 2) AS revenue_per_vehicle,
    ROUND(total_distance / NULLIF(active_vehicles, 0), 1) AS distance_per_vehicle

FROM current_period