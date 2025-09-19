-- Real-time recommendations for operations team
SELECT 
    recommendation_id,
    recommendation_type, -- 'route_optimization', 'vehicle_assignment', 'maintenance_schedule'
    priority_level,
    
    -- Context
    affected_shipments,
    estimated_cost_impact,
    implementation_urgency,
    
    -- Supporting data
    weather_factors,
    traffic_conditions,
    vehicle_availability,
    
    -- ML confidence scores
    model_confidence_score,
    historical_accuracy_rate