-- Real-time recommendations for operations team
CREATE OR REPLACE VIEW ANALYTICS.view_ai_recommendations AS
WITH route_optimization_recommendations AS (
    SELECT 
        'ROUTE_OPT_' || route_id as recommendation_id,
        'route_optimization' as recommendation_type,
        CASE 
            WHEN avg_delay_minutes > 60 THEN 'HIGH'
            WHEN avg_delay_minutes > 30 THEN 'MEDIUM'
            ELSE 'LOW'
        END as priority_level,
        COUNT(*) as affected_shipments,
        SUM(estimated_cost_impact_usd) as estimated_cost_impact,
        CASE 
            WHEN avg_delay_minutes > 60 THEN 'IMMEDIATE'
            WHEN avg_delay_minutes > 30 THEN 'WITHIN_HOUR'
            ELSE 'WITHIN_DAY'
        END as implementation_urgency,
        AVG(weather_delay_minutes) as weather_factors,
        AVG(traffic_delay_minutes) as traffic_conditions,
        COUNT(DISTINCT vehicle_id) as vehicle_availability,
        0.85 as model_confidence_score,
        0.78 as historical_accuracy_rate
    FROM {{ ref('fact_shipments') }}
    WHERE shipment_date >= CURRENT_DATE() - 7
    GROUP BY route_id
    HAVING COUNT(*) > 10
),
vehicle_assignment_recommendations AS (
    SELECT 
        'VEHICLE_ASSIGN_' || vehicle_id as recommendation_id,
        'vehicle_assignment' as recommendation_type,
        CASE 
            WHEN utilization_rate < 0.3 THEN 'HIGH'
            WHEN utilization_rate < 0.5 THEN 'MEDIUM'
            ELSE 'LOW'
        END as priority_level,
        COUNT(*) as affected_shipments,
        SUM(estimated_cost_impact_usd) as estimated_cost_impact,
        'WITHIN_DAY' as implementation_urgency,
        0 as weather_factors,
        0 as traffic_conditions,
        1 as vehicle_availability,
        0.92 as model_confidence_score,
        0.85 as historical_accuracy_rate
    FROM {{ ref('fact_vehicle_utilization') }}
    WHERE date_key >= CURRENT_DATE() - 7
    GROUP BY vehicle_id
    HAVING COUNT(*) > 5
),
maintenance_schedule_recommendations AS (
    SELECT 
        'MAINT_SCHED_' || vehicle_id as recommendation_id,
        'maintenance_schedule' as recommendation_type,
        CASE 
            WHEN risk_score > 80 THEN 'HIGH'
            WHEN risk_score > 60 THEN 'MEDIUM'
            ELSE 'LOW'
        END as priority_level,
        1 as affected_shipments,
        maintenance_cost_usd as estimated_cost_impact,
        CASE 
            WHEN risk_score > 80 THEN 'IMMEDIATE'
            WHEN risk_score > 60 THEN 'WITHIN_WEEK'
            ELSE 'WITHIN_MONTH'
        END as implementation_urgency,
        0 as weather_factors,
        0 as traffic_conditions,
        1 as vehicle_availability,
        0.88 as model_confidence_score,
        0.82 as historical_accuracy_rate
    FROM {{ ref('dim_vehicle_maintenance') }}
    WHERE maintenance_status = 'SCHEDULED'
        AND maintenance_date <= CURRENT_DATE() + 30
)
SELECT 
    recommendation_id,
    recommendation_type,
    priority_level,
    affected_shipments,
    estimated_cost_impact,
    implementation_urgency,
    weather_factors,
    traffic_conditions,
    vehicle_availability,
    model_confidence_score,
    historical_accuracy_rate,
    CURRENT_TIMESTAMP() as recommendation_created_at
FROM route_optimization_recommendations
UNION ALL
SELECT 
    recommendation_id,
    recommendation_type,
    priority_level,
    affected_shipments,
    estimated_cost_impact,
    implementation_urgency,
    weather_factors,
    traffic_conditions,
    vehicle_availability,
    model_confidence_score,
    historical_accuracy_rate,
    CURRENT_TIMESTAMP() as recommendation_created_at
FROM vehicle_assignment_recommendations
UNION ALL
SELECT 
    recommendation_id,
    recommendation_type,
    priority_level,
    affected_shipments,
    estimated_cost_impact,
    implementation_urgency,
    weather_factors,
    traffic_conditions,
    vehicle_availability,
    model_confidence_score,
    historical_accuracy_rate,
    CURRENT_TIMESTAMP() as recommendation_created_at
FROM maintenance_schedule_recommendations
ORDER BY priority_level DESC, model_confidence_score DESC