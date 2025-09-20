-- =====================================================
-- Business Intelligence Views
-- =====================================================

-- AI Recommendations
{{ config(
    materialized='view',
    tags=['marts', 'analytics', 'bi', 'ai', 'recommendations', 'load_third']
) }}

WITH route_performance_analysis AS (
    SELECT 
        dr.route_id,
        dr.route_id as route_name,
        null as route_type,
        dr.distance_km as total_distance_km,
        null as complexity_score,
        
        COUNT(*) AS total_trips_last_30d,
        AVG(fs.actual_duration_minutes) AS avg_actual_duration,
        AVG(fs.planned_duration_minutes) AS avg_planned_duration,
        AVG(CASE WHEN fs.is_on_time = true THEN 1.0 ELSE 0.0 END) AS on_time_rate,
        AVG(fs.fuel_cost) AS avg_fuel_cost,
        AVG(fs.route_efficiency_score) AS avg_customer_rating,
        STDDEV(fs.actual_duration_minutes) AS duration_variability,
        
        -- Efficiency calculations
        AVG(fs.actual_duration_minutes / NULLIF(fs.planned_duration_minutes, 1)) AS avg_duration_ratio,
        SUM(fs.revenue - fs.delivery_cost - fs.fuel_cost) / COUNT(*) AS avg_profit_per_trip
        
    FROM {{ ref('tbl_dim_route') }} dr
    JOIN {{ ref('tbl_fact_shipments') }} fs ON dr.route_id = fs.route_id
    WHERE fs.shipment_date >= CURRENT_DATE() - 30
        AND fs.is_delivered = TRUE
    GROUP BY 1,2,3,4,5
),

vehicle_utilization_analysis AS (
    SELECT 
        dv.vehicle_id,
        dv.vehicle_type,
        dv.capacity_kg,
        dv.current_mileage as condition_score,
        
        COUNT(*) AS trips_last_30d,
        AVG(fs.weight_kg / NULLIF(dv.capacity_kg, 1)) AS avg_capacity_utilization,
        SUM(fs.distance_km) AS total_distance_30d,
        AVG(fs.fuel_cost / NULLIF(fs.distance_km, 0)) AS fuel_efficiency,
        AVG(CASE WHEN fs.is_on_time THEN 1.0 ELSE 0.0 END) AS vehicle_on_time_rate,
        
        -- Maintenance indicators
        DATEDIFF(day, dv.last_maintenance_date, CURRENT_DATE()) AS days_since_service,
        CASE WHEN dv.next_maintenance_date < CURRENT_DATE() THEN 'overdue'
             WHEN dv.next_maintenance_date <= CURRENT_DATE() + 7 THEN 'due_soon'
             ELSE 'current' END AS service_status
        
    FROM {{ ref('tbl_dim_vehicle') }} dv
    JOIN {{ ref('tbl_fact_shipments') }} fs ON dv.vehicle_id = fs.vehicle_id
    WHERE fs.shipment_date >= CURRENT_DATE() - 30
        AND dv.vehicle_status = 'ACTIVE'
    GROUP BY dv.vehicle_id, dv.vehicle_type, dv.capacity_kg, dv.current_mileage, dv.last_maintenance_date, dv.next_maintenance_date
),

customer_insights AS (
    SELECT 
        dc.customer_id,
        dc.customer_name,
        dc.customer_tier as volume_segment,
        
        COUNT(*) AS shipments_last_30d,
        AVG(fs.route_efficiency_score) AS avg_satisfaction,
        AVG(CASE WHEN fs.is_on_time THEN 1.0 ELSE 0.0 END) AS customer_on_time_rate,
        SUM(fs.revenue) AS revenue_last_30d,
        
        -- Growth indicators
        LAG(COUNT(*), 30) OVER (PARTITION BY dc.customer_id ORDER BY MIN(fs.shipment_date)) AS shipments_30d_ago,
        COUNT(*) - LAG(COUNT(*), 30) OVER (PARTITION BY dc.customer_id ORDER BY MIN(fs.shipment_date)) AS shipment_growth
        
    FROM {{ ref('tbl_dim_customer') }} dc
    JOIN {{ ref('tbl_fact_shipments') }} fs ON dc.customer_id = fs.customer_id
    WHERE fs.shipment_date >= CURRENT_DATE() - 60  -- Extended window for comparison
    GROUP BY 1,2,3
)

-- Route optimization recommendations
SELECT 
    'route_optimization' AS recommendation_type,
    rpa.route_id AS entity_id,
    rpa.route_name AS entity_name,
    'high' AS priority_level,
    ROUND(rpa.avg_duration_ratio * 100 - 100, 1) || '% over planned time' AS issue_description,
    'Optimize route planning or adjust estimated times' AS recommendation,
    ROUND((rpa.avg_actual_duration - rpa.avg_planned_duration) * rpa.total_trips_last_30d / 60.0, 1) AS impact_hours_saved,
    ROUND(rpa.avg_profit_per_trip * 0.1 * rpa.total_trips_last_30d, 2) AS estimated_cost_impact,
    rpa.on_time_rate AS confidence_score,
    CURRENT_TIMESTAMP() AS generated_at
FROM route_performance_analysis rpa
WHERE rpa.avg_duration_ratio > 1.2
    AND rpa.total_trips_last_30d >= 5

UNION ALL

-- Vehicle assignment recommendations
SELECT 
    'vehicle_assignment' AS recommendation_type,
    vua.vehicle_id AS entity_id,
    vua.vehicle_type AS entity_name,
    CASE WHEN vua.avg_capacity_utilization < 0.4 THEN 'medium'
         ELSE 'low' END AS priority_level,
    'Vehicle underutilized at ' || ROUND(vua.avg_capacity_utilization * 100, 1) || '% capacity' AS issue_description,
    CASE WHEN vua.avg_capacity_utilization < 0.4 THEN 'Consider route consolidation or smaller vehicle'
         ELSE 'Monitor for optimization opportunities' END AS recommendation,
    ROUND((0.6 - vua.avg_capacity_utilization) * vua.total_distance_30d, 1) AS impact_km_optimization,
    ROUND(vua.fuel_efficiency * vua.total_distance_30d * 0.2, 2) AS estimated_cost_impact,
    1 - vua.avg_capacity_utilization AS confidence_score,
    CURRENT_TIMESTAMP() AS generated_at
FROM vehicle_utilization_analysis vua
WHERE vua.avg_capacity_utilization < 0.6
    AND vua.trips_last_30d >= 10

UNION ALL

-- Maintenance schedule recommendations
SELECT 
    'maintenance_schedule' AS recommendation_type,
    vua.vehicle_id AS entity_id,
    vua.vehicle_type AS entity_name,
    CASE WHEN vua.service_status = 'overdue' THEN 'urgent'
         WHEN vua.service_status = 'due_soon' THEN 'high'
         ELSE 'medium' END AS priority_level,
    'Service ' || vua.service_status || ' - ' || vua.days_since_service || ' days since last service' AS issue_description,
    CASE WHEN vua.service_status = 'overdue' THEN 'Schedule immediate maintenance'
         WHEN vua.service_status = 'due_soon' THEN 'Schedule maintenance within 7 days'
         ELSE 'Plan upcoming maintenance' END AS recommendation,
    ROUND(vua.vehicle_on_time_rate * 100, 1) AS impact_on_time_rate,
    ROUND(vua.condition_score * 1000, 2) AS estimated_cost_impact,
    CASE WHEN vua.service_status = 'overdue' THEN 0.9
         WHEN vua.service_status = 'due_soon' THEN 0.8
         ELSE 0.6 END AS confidence_score,
    CURRENT_TIMESTAMP() AS generated_at
FROM vehicle_utilization_analysis vua
WHERE vua.service_status IN ('overdue', 'due_soon')

UNION ALL

-- Customer retention recommendations
SELECT 
    'customer_retention' AS recommendation_type,
    ci.customer_id AS entity_id,
    ci.customer_name AS entity_name,
    CASE WHEN ci.avg_satisfaction < 6 THEN 'urgent'
         WHEN ci.customer_on_time_rate < 0.7 THEN 'high'
         ELSE 'medium' END AS priority_level,
    'Satisfaction: ' || ROUND(ci.avg_satisfaction, 1) || '/10, On-time: ' || ROUND(ci.customer_on_time_rate * 100, 1) || '%' AS issue_description,
    CASE WHEN ci.avg_satisfaction < 6 THEN 'Immediate customer outreach and service recovery'
         WHEN ci.customer_on_time_rate < 0.7 THEN 'Improve delivery reliability for this customer'
         ELSE 'Monitor service quality' END AS recommendation,
    ci.revenue_last_30d AS impact_revenue_at_risk,
    ci.revenue_last_30d * 0.1 AS estimated_cost_impact,
    CASE WHEN ci.avg_satisfaction < 6 THEN 0.9
         WHEN ci.customer_on_time_rate < 0.7 THEN 0.8
         ELSE 0.6 END AS confidence_score,
    CURRENT_TIMESTAMP() AS generated_at
FROM customer_insights ci
WHERE (ci.avg_satisfaction < 7 OR ci.customer_on_time_rate < 0.8)
    AND ci.shipments_last_30d >= 5
