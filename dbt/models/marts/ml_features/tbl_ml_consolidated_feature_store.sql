-- =====================================================
-- Consolidated ML Feature Store
-- Simplified version using fact tables as primary source
-- =====================================================

{{ config(
    materialized='table',
    tags=['marts', 'ml_features', 'ml', 'features', 'feature_store', 'consolidated', 'ml_optimized', 'load_third']
) }}

WITH shipment_features AS (
    SELECT 
        shipment_id as entity_id,
        'shipment' as entity_type,
        shipment_date as feature_date,
        
        -- Basic shipment features
        fs.customer_id,
        fs.vehicle_id,
        fs.route_id,
        fs.distance_km,
        fs.weight_kg,
        fs.priority_level,
        fs.planned_duration_minutes,
        fs.actual_duration_minutes,
        fs.is_on_time,
        fs.route_efficiency_score,
        fs.fuel_cost,
        fs.delivery_cost,
        fs.revenue,
        fs.vehicle_type,
        
        -- Derived features
        CASE 
            WHEN fs.distance_km < 50 THEN 'SHORT_HAUL'
            WHEN fs.distance_km < 200 THEN 'MEDIUM_HAUL'
            ELSE 'LONG_HAUL'
        END as haul_type,
        
        CASE 
            WHEN fs.actual_duration_minutes > fs.planned_duration_minutes * 1.5 THEN 'SEVERE_DELAY'
            WHEN fs.actual_duration_minutes > fs.planned_duration_minutes * 1.2 THEN 'MODERATE_DELAY'
            WHEN fs.actual_duration_minutes < fs.planned_duration_minutes * 0.8 THEN 'EARLY'
            ELSE 'ON_TIME'
        END as delivery_performance,
        
        -- Performance metrics
        CASE WHEN fs.is_on_time THEN 1.0 ELSE 0.0 END as on_time_flag,
        (fs.revenue - fs.delivery_cost - fs.fuel_cost) as profit_margin,
        fs.fuel_cost / NULLIF(fs.distance_km, 0) as cost_per_km,
        
        -- Customer tier (from dimension)
        dc.customer_tier,
        dc.credit_limit_usd,
        
        -- Vehicle features (from dimension)
        dv.capacity_kg,
        dv.fuel_efficiency_mpg,
        dv.model_year,
        dv.current_mileage
        
    FROM {{ ref('tbl_fact_shipments') }} fs
    LEFT JOIN {{ ref('tbl_dim_customer') }} dc ON fs.customer_id = dc.customer_id
    LEFT JOIN {{ ref('tbl_dim_vehicle') }} dv ON fs.vehicle_id = dv.vehicle_id
    WHERE fs.shipment_date >= CURRENT_DATE() - 90  -- Last 90 days
),

vehicle_telemetry_features AS (
    SELECT 
        vehicle_id as entity_id,
        'vehicle' as entity_type,
        DATE(timestamp) as feature_date,
        
        -- Telemetry features
        AVG(engine_temp_c) as avg_engine_temp,
        AVG(fuel_level_percent) as avg_fuel_level,
        AVG(engine_rpm) as avg_engine_rpm,
        SUM(harsh_braking_events) as total_harsh_braking,
        SUM(harsh_acceleration_events) as total_harsh_acceleration,
        SUM(speeding_events) as total_speeding_events,
        AVG(idle_time_minutes) as avg_idle_time,
        AVG(engine_health_score) as avg_engine_health,
        COUNT(*) as telemetry_records_count
        
    FROM {{ ref('tbl_fact_vehicle_telemetry') }}
    WHERE timestamp >= CURRENT_DATE() - 30  -- Last 30 days
    GROUP BY vehicle_id, DATE(timestamp)
)

-- Union all feature types
SELECT * FROM shipment_features
UNION ALL
SELECT 
    entity_id,
    entity_type,
    feature_date,
    null as customer_id,
    entity_id as vehicle_id,
    null as route_id,
    null as distance_km,
    null as weight_kg,
    null as priority_level,
    null as planned_duration_minutes,
    null as actual_duration_minutes,
    null as is_on_time,
    null as route_efficiency_score,
    null as fuel_cost,
    null as delivery_cost,
    null as revenue,
    null as vehicle_type,
    null as haul_type,
    null as delivery_performance,
    null as on_time_flag,
    null as profit_margin,
    null as cost_per_km,
    null as customer_tier,
    null as credit_limit_usd,
    null as capacity_kg,
    null as fuel_efficiency_mpg,
    null as model_year,
    null as current_mileage
FROM vehicle_telemetry_features