-- Test Analytics View Consistency
-- Validates that analytics views produce consistent results with underlying data

WITH performance_dashboard_validation AS (
    SELECT 
        delivery_date,
        haul_type,
        branch_location,
        total_deliveries,
        on_time_rate,
        avg_satisfaction,
        total_cost,
        route_efficiency,
        fleet_utilization,
        forecast_delay,
        maintenance_alerts,
        total_revenue,
        avg_profit_margin,
        total_carbon_emissions,
        active_vehicles,
        active_customers,
        
        -- Validate on-time rate is between 0 and 1
        CASE 
            WHEN on_time_rate < 0 OR on_time_rate > 1 THEN 1 
            ELSE 0 
        END as invalid_on_time_rate,
        
        -- Validate satisfaction rating is between 1 and 5
        CASE 
            WHEN avg_satisfaction < 1 OR avg_satisfaction > 5 THEN 1 
            ELSE 0 
        END as invalid_satisfaction,
        
        -- Validate route efficiency is between 0 and 100
        CASE 
            WHEN route_efficiency < 0 OR route_efficiency > 100 THEN 1 
            ELSE 0 
        END as invalid_route_efficiency,
        
        -- Validate fleet utilization is between 0 and 1
        CASE 
            WHEN fleet_utilization < 0 OR fleet_utilization > 1 THEN 1 
            ELSE 0 
        END as invalid_fleet_utilization,
        
        -- Validate profit margin is reasonable
        CASE 
            WHEN avg_profit_margin < -100 OR avg_profit_margin > 100 THEN 1 
            ELSE 0 
        END as invalid_profit_margin,
        
        -- Validate positive counts
        CASE 
            WHEN total_deliveries < 0 OR active_vehicles < 0 OR active_customers < 0 THEN 1 
            ELSE 0 
        END as invalid_counts,
        
        -- Validate positive costs and revenue
        CASE 
            WHEN total_cost < 0 OR total_revenue < 0 THEN 1 
            ELSE 0 
        END as invalid_financials
        
    FROM {{ ref('view_performance_dashboard') }}
    WHERE delivery_date >= DATEADD('day', -30, CURRENT_DATE())
),
ai_recommendations_validation AS (
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
        recommendation_created_at,
        
        -- Validate recommendation type
        CASE 
            WHEN recommendation_type NOT IN ('route_optimization', 'vehicle_assignment', 'maintenance_schedule') THEN 1 
            ELSE 0 
        END as invalid_recommendation_type,
        
        -- Validate priority level
        CASE 
            WHEN priority_level NOT IN ('HIGH', 'MEDIUM', 'LOW') THEN 1 
            ELSE 0 
        END as invalid_priority_level,
        
        -- Validate urgency
        CASE 
            WHEN implementation_urgency NOT IN ('IMMEDIATE', 'WITHIN_HOUR', 'WITHIN_DAY', 'WITHIN_WEEK', 'WITHIN_MONTH') THEN 1 
            ELSE 0 
        END as invalid_urgency,
        
        -- Validate confidence scores
        CASE 
            WHEN model_confidence_score < 0 OR model_confidence_score > 1 THEN 1 
            ELSE 0 
        END as invalid_confidence_score,
        
        -- Validate accuracy rate
        CASE 
            WHEN historical_accuracy_rate < 0 OR historical_accuracy_rate > 1 THEN 1 
            ELSE 0 
        END as invalid_accuracy_rate,
        
        -- Validate positive counts
        CASE 
            WHEN affected_shipments < 0 OR vehicle_availability < 0 THEN 1 
            ELSE 0 
        END as invalid_counts
        
    FROM {{ ref('view_ai_recommendations') }}
    WHERE recommendation_created_at >= DATEADD('day', -7, CURRENT_DATE())
)
SELECT 
    'performance_dashboard' as view_name,
    delivery_date as record_date,
    invalid_on_time_rate,
    invalid_satisfaction,
    invalid_route_efficiency,
    invalid_fleet_utilization,
    invalid_profit_margin,
    invalid_counts,
    invalid_financials
FROM performance_dashboard_validation
WHERE 
    invalid_on_time_rate = 1
    OR invalid_satisfaction = 1
    OR invalid_route_efficiency = 1
    OR invalid_fleet_utilization = 1
    OR invalid_profit_margin = 1
    OR invalid_counts = 1
    OR invalid_financials = 1

UNION ALL

SELECT 
    'ai_recommendations' as view_name,
    recommendation_created_at as record_date,
    invalid_recommendation_type,
    invalid_priority_level,
    invalid_urgency,
    invalid_confidence_score,
    invalid_accuracy_rate,
    invalid_counts,
    0 as invalid_financials
FROM ai_recommendations_validation
WHERE 
    invalid_recommendation_type = 1
    OR invalid_priority_level = 1
    OR invalid_urgency = 1
    OR invalid_confidence_score = 1
    OR invalid_accuracy_rate = 1
    OR invalid_counts = 1
