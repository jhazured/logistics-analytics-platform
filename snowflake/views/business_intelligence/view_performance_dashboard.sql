-- Executive dashboard aggregations
SELECT 
    branch_location,
    haul_type,
    delivery_date,
    
    -- KPIs by segment
    COUNT(*) as total_deliveries,
    AVG(on_time_delivery_flag) as on_time_rate,
    AVG(customer_satisfaction_score) as avg_satisfaction,
    SUM(delivery_cost_aud) as total_cost,
    
    -- Efficiency metrics
    AVG(route_efficiency_index) as route_efficiency,
    AVG(vehicle_utilization_rate) as fleet_utilization,
    
    -- Predictive insights
    AVG(predicted_delay_minutes) as forecast_delay,
    COUNT(CASE WHEN maintenance_alert = 1 THEN 1 END) as maintenance_alerts