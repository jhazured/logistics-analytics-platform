-- Executive dashboard aggregations
CREATE OR REPLACE VIEW ANALYTICS.view_performance_dashboard AS
WITH daily_performance AS (
    SELECT 
        s.shipment_date as delivery_date,
        c.segment as haul_type,
        l.city as branch_location,
        
        -- KPIs by segment
        COUNT(*) as total_deliveries,
        AVG(CASE WHEN s.delivery_status = 'DELIVERED' AND s.actual_delivery_date <= s.planned_delivery_date THEN 1 ELSE 0 END) as on_time_rate,
        AVG(COALESCE(s.customer_rating, 4.0)) as avg_satisfaction,
        SUM(s.total_cost) as total_cost,
        
        -- Efficiency metrics
        AVG(s.route_efficiency_score) as route_efficiency,
        AVG(v.utilization_score) as fleet_utilization,
        
        -- Predictive insights
        AVG(s.weather_delay_minutes + s.traffic_delay_minutes) as forecast_delay,
        COUNT(CASE WHEN vm.maintenance_alert = true THEN 1 END) as maintenance_alerts,
        
        -- Additional metrics
        SUM(s.revenue) as total_revenue,
        AVG(s.profit_margin_pct) as avg_profit_margin,
        SUM(s.carbon_emissions_kg) as total_carbon_emissions,
        COUNT(DISTINCT s.vehicle_id) as active_vehicles,
        COUNT(DISTINCT s.customer_id) as active_customers
        
    FROM {{ ref('fact_shipments') }} s
    LEFT JOIN {{ ref('dim_customer') }} c ON s.customer_id = c.customer_id
    LEFT JOIN {{ ref('dim_location') }} l ON s.origin_location_id = l.location_id
    LEFT JOIN {{ ref('fact_vehicle_utilization') }} v ON s.vehicle_id = v.vehicle_id AND s.shipment_date = v.date_key
    LEFT JOIN {{ ref('fact_vehicle_telemetry') }} vm ON s.vehicle_id = vm.vehicle_id
    WHERE s.shipment_date >= CURRENT_DATE() - 30
    GROUP BY s.shipment_date, c.segment, l.city
),
rolling_metrics AS (
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
        
        -- 7-day rolling averages
        AVG(on_time_rate) OVER (
            PARTITION BY haul_type, branch_location 
            ORDER BY delivery_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7d_on_time_rate,
        
        AVG(route_efficiency) OVER (
            PARTITION BY haul_type, branch_location 
            ORDER BY delivery_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7d_route_efficiency,
        
        AVG(fleet_utilization) OVER (
            PARTITION BY haul_type, branch_location 
            ORDER BY delivery_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7d_fleet_utilization,
        
        -- 30-day rolling averages
        AVG(on_time_rate) OVER (
            PARTITION BY haul_type, branch_location 
            ORDER BY delivery_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as rolling_30d_on_time_rate,
        
        AVG(route_efficiency) OVER (
            PARTITION BY haul_type, branch_location 
            ORDER BY delivery_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as rolling_30d_route_efficiency,
        
        AVG(fleet_utilization) OVER (
            PARTITION BY haul_type, branch_location 
            ORDER BY delivery_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as rolling_30d_fleet_utilization
        
    FROM daily_performance
)
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
    
    -- Rolling metrics
    rolling_7d_on_time_rate,
    rolling_7d_route_efficiency,
    rolling_7d_fleet_utilization,
    rolling_30d_on_time_rate,
    rolling_30d_route_efficiency,
    rolling_30d_fleet_utilization,
    
    -- Performance indicators
    CASE 
        WHEN rolling_30d_on_time_rate > 0.95 THEN 'EXCELLENT'
        WHEN rolling_30d_on_time_rate > 0.85 THEN 'GOOD'
        WHEN rolling_30d_on_time_rate > 0.70 THEN 'FAIR'
        ELSE 'POOR'
    END as performance_rating,
    
    CASE 
        WHEN rolling_30d_route_efficiency > 80 THEN 'HIGH_EFFICIENCY'
        WHEN rolling_30d_route_efficiency > 60 THEN 'MEDIUM_EFFICIENCY'
        ELSE 'LOW_EFFICIENCY'
    END as efficiency_rating,
    
    CASE 
        WHEN rolling_30d_fleet_utilization > 0.8 THEN 'HIGH_UTILIZATION'
        WHEN rolling_30d_fleet_utilization > 0.6 THEN 'MEDIUM_UTILIZATION'
        ELSE 'LOW_UTILIZATION'
    END as utilization_rating,
    
    -- Trend indicators
    CASE 
        WHEN rolling_7d_on_time_rate > rolling_30d_on_time_rate THEN 'IMPROVING'
        WHEN rolling_7d_on_time_rate < rolling_30d_on_time_rate THEN 'DECLINING'
        ELSE 'STABLE'
    END as on_time_trend,
    
    CASE 
        WHEN rolling_7d_route_efficiency > rolling_30d_route_efficiency THEN 'IMPROVING'
        WHEN rolling_7d_route_efficiency < rolling_30d_route_efficiency THEN 'DECLINING'
        ELSE 'STABLE'
    END as efficiency_trend,
    
    CURRENT_TIMESTAMP() as dashboard_updated_at

FROM rolling_metrics
ORDER BY delivery_date DESC, haul_type, branch_location