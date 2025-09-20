-- =====================================================
-- Consolidated ML Feature Store
-- Combines feature store, real-time scoring, and route optimization features
-- =====================================================

{{ config(
    materialized='table',
    cluster_by=['entity_type', 'entity_id', 'feature_date'],
    tags=['ml', 'features', 'feature_store', 'consolidated', 'ml_optimized']
) }}

WITH customer_features AS (
    SELECT 
        customer_id,
        customer_tier,
        segment,
        credit_limit_usd,
        payment_terms,
        customer_since_date,
        DATEDIFF('day', customer_since_date, CURRENT_DATE()) as customer_tenure_days,
        CASE 
            WHEN customer_tier = 'PREMIUM' THEN 3
            WHEN customer_tier = 'STANDARD' THEN 2
            WHEN customer_tier = 'BASIC' THEN 1
            ELSE 0
        END as customer_tier_numeric
    FROM {{ ref('dim_customer') }}
    WHERE is_active = true
),

vehicle_features AS (
    SELECT 
        vehicle_id,
        vehicle_type,
        model_year,
        capacity_kg,
        fuel_efficiency_mpg,
        maintenance_interval_miles,
        current_mileage,
        vehicle_status,
        DATEDIFF('year', DATE(model_year || '-01-01'), CURRENT_DATE()) as vehicle_age_years,
        CASE 
            WHEN vehicle_type = 'TRUCK' THEN 1
            WHEN vehicle_type = 'VAN' THEN 2
            WHEN vehicle_type = 'MOTORCYCLE' THEN 3
            ELSE 4
        END as vehicle_type_numeric
    FROM {{ ref('dim_vehicle') }}
    WHERE vehicle_status = 'ACTIVE'
),

route_features AS (
    SELECT 
        route_id,
        route_name,
        route_type,
        total_distance_km,
        estimated_duration_minutes,
        complexity_score,
        traffic_density,
        weather_risk,
        CASE 
            WHEN total_distance_km < 50 THEN 'SHORT_HAUL'
            WHEN total_distance_km < 200 THEN 'MEDIUM_HAUL'
            ELSE 'LONG_HAUL'
        END as haul_type
    FROM {{ ref('dim_route') }}
),

-- Customer behavioral features
customer_behavioral_features AS (
    SELECT 
        customer_id,
        feature_date,
        -- Rolling metrics from consolidated rolling analytics
        daily_shipments,
        daily_revenue,
        shipments_30d_avg,
        revenue_30d_avg,
        shipments_7d_avg,
        shipments_90d_avg,
        activity_ratio_7d_vs_90d,
        revenue_yoy_growth_percent,
        shipment_volatility_30d,
        behavior_consistency
    FROM {{ ref('ml_rolling_analytics') }}
    WHERE entity_type = 'customer'
),

-- Vehicle performance features
vehicle_performance_features AS (
    SELECT 
        vehicle_id,
        feature_date,
        -- Rolling metrics from consolidated rolling analytics
        daily_deliveries,
        daily_distance_km,
        daily_fuel_cost,
        daily_on_time_rate,
        daily_avg_rating,
        deliveries_7d_avg,
        deliveries_30d_avg,
        on_time_rate_7d_avg,
        on_time_rate_30d_avg,
        fuel_efficiency_7d_avg,
        fuel_efficiency_30d_avg,
        performance_trend_7d_vs_30d,
        fuel_efficiency_trend_7d_vs_30d
    FROM {{ ref('ml_rolling_analytics') }}
    WHERE entity_type = 'vehicle'
),

-- Route performance features
route_performance_features AS (
    SELECT 
        route_id,
        feature_date,
        -- Rolling metrics from consolidated rolling analytics
        daily_trips,
        daily_on_time_rate,
        avg_customer_satisfaction,
        total_fuel_cost,
        total_revenue,
        avg_fuel_cost_per_km,
        on_time_rate_7d_avg,
        on_time_rate_30d_avg,
        satisfaction_7d_avg,
        satisfaction_30d_avg,
        route_performance_rating,
        route_risk_level
    FROM {{ ref('ml_rolling_analytics') }}
    WHERE entity_type = 'route'
),

-- Maintenance features
maintenance_features AS (
    SELECT 
        vehicle_id,
        feature_date,
        -- Features from consolidated maintenance model
        avg_engine_temp,
        avg_engine_health,
        total_harsh_braking,
        total_harsh_acceleration,
        maintenance_events_30d,
        maintenance_cost_30d,
        maintenance_urgency,
        days_since_last_maintenance,
        miles_since_last_maintenance,
        predictive_maintenance_score,
        maintenance_risk_score,
        next_maintenance_due_days
    FROM {{ ref('ml_maintenance_features') }}
),

-- Current shipment features for real-time scoring
current_shipment_features AS (
    SELECT 
        fs.shipment_id,
        fs.route_id,
        fs.vehicle_id,
        fs.customer_id,
        fs.shipment_date,
        fs.planned_delivery_date,
        fs.delivery_status,
        dr.total_distance_km,
        dr.complexity_score,
        dv.vehicle_type,
        dc.customer_tier,
        
        -- Real-time features
        EXTRACT(hour FROM CURRENT_TIMESTAMP()) AS current_hour,
        EXTRACT(dayofweek FROM CURRENT_DATE()) AS current_dow,
        CASE 
            WHEN dr.total_distance_km < 50 THEN 'SHORT_HAUL'
            WHEN dr.total_distance_km < 200 THEN 'MEDIUM_HAUL'
            ELSE 'LONG_HAUL'
        END AS haul_type
        
    FROM {{ ref('fact_shipments') }} fs
    JOIN {{ ref('dim_route') }} dr ON fs.route_id = dr.route_id
    JOIN {{ ref('dim_vehicle') }} dv ON fs.vehicle_id = dv.vehicle_id
    JOIN {{ ref('dim_customer') }} dc ON fs.customer_id = dc.customer_id
    WHERE fs.shipment_date = CURRENT_DATE()
        AND fs.delivery_status = 'IN_TRANSIT'
)

-- Customer feature store
SELECT 
    'customer' AS entity_type,
    cf.customer_id AS entity_id,
    COALESCE(cbf.feature_date, CURRENT_DATE()) AS feature_date,
    
    -- Static customer features
    cf.customer_tier,
    cf.segment,
    cf.credit_limit_usd,
    cf.payment_terms,
    cf.customer_since_date,
    cf.customer_tenure_days,
    cf.customer_tier_numeric,
    
    -- Behavioral features
    COALESCE(cbf.daily_shipments, 0) AS daily_shipments,
    COALESCE(cbf.daily_revenue, 0) AS daily_revenue,
    COALESCE(cbf.shipments_30d_avg, 0) AS shipments_30d_avg,
    COALESCE(cbf.revenue_30d_avg, 0) AS revenue_30d_avg,
    COALESCE(cbf.shipments_7d_avg, 0) AS shipments_7d_avg,
    COALESCE(cbf.shipments_90d_avg, 0) AS shipments_90d_avg,
    COALESCE(cbf.activity_ratio_7d_vs_90d, 0) AS activity_ratio_7d_vs_90d,
    COALESCE(cbf.revenue_yoy_growth_percent, 0) AS revenue_yoy_growth_percent,
    COALESCE(cbf.shipment_volatility_30d, 0) AS shipment_volatility_30d,
    COALESCE(cbf.behavior_consistency, 'unknown') AS behavior_consistency,
    
    -- Null fields for other entity types
    NULL AS vehicle_type,
    NULL AS model_year,
    NULL AS capacity_kg,
    NULL AS fuel_efficiency_mpg,
    NULL AS maintenance_interval_miles,
    NULL AS current_mileage,
    NULL AS vehicle_status,
    NULL AS vehicle_age_years,
    NULL AS vehicle_type_numeric,
    NULL AS route_name,
    NULL AS route_type,
    NULL AS total_distance_km,
    NULL AS estimated_duration_minutes,
    NULL AS complexity_score,
    NULL AS traffic_density,
    NULL AS weather_risk,
    NULL AS haul_type,
    NULL AS daily_deliveries,
    NULL AS daily_distance_km,
    NULL AS daily_fuel_cost,
    NULL AS daily_on_time_rate,
    NULL AS daily_avg_rating,
    NULL AS deliveries_7d_avg,
    NULL AS deliveries_30d_avg,
    NULL AS on_time_rate_7d_avg,
    NULL AS on_time_rate_30d_avg,
    NULL AS fuel_efficiency_7d_avg,
    NULL AS fuel_efficiency_30d_avg,
    NULL AS performance_trend_7d_vs_30d,
    NULL AS fuel_efficiency_trend_7d_vs_30d,
    NULL AS daily_trips,
    NULL AS avg_customer_satisfaction,
    NULL AS total_fuel_cost,
    NULL AS total_revenue,
    NULL AS avg_fuel_cost_per_km,
    NULL AS satisfaction_7d_avg,
    NULL AS satisfaction_30d_avg,
    NULL AS route_performance_rating,
    NULL AS route_risk_level,
    NULL AS avg_engine_temp,
    NULL AS avg_engine_health,
    NULL AS total_harsh_braking,
    NULL AS total_harsh_acceleration,
    NULL AS maintenance_events_30d,
    NULL AS maintenance_cost_30d,
    NULL AS maintenance_urgency,
    NULL AS days_since_last_maintenance,
    NULL AS miles_since_last_maintenance,
    NULL AS predictive_maintenance_score,
    NULL AS maintenance_risk_score,
    NULL AS next_maintenance_due_days,
    NULL AS shipment_id,
    NULL AS planned_delivery_date,
    NULL AS delivery_status,
    NULL AS current_hour,
    NULL AS current_dow

FROM customer_features cf
LEFT JOIN customer_behavioral_features cbf ON cf.customer_id = cbf.customer_id

UNION ALL

-- Vehicle feature store
SELECT 
    'vehicle' AS entity_type,
    vf.vehicle_id AS entity_id,
    COALESCE(vpf.feature_date, CURRENT_DATE()) AS feature_date,
    
    -- Static vehicle features
    NULL AS customer_tier,
    NULL AS segment,
    NULL AS credit_limit_usd,
    NULL AS payment_terms,
    NULL AS customer_since_date,
    NULL AS customer_tenure_days,
    NULL AS customer_tier_numeric,
    
    -- Null behavioral features
    NULL AS daily_shipments,
    NULL AS daily_revenue,
    NULL AS shipments_30d_avg,
    NULL AS revenue_30d_avg,
    NULL AS shipments_7d_avg,
    NULL AS shipments_90d_avg,
    NULL AS activity_ratio_7d_vs_90d,
    NULL AS revenue_yoy_growth_percent,
    NULL AS shipment_volatility_30d,
    NULL AS behavior_consistency,
    
    -- Vehicle features
    vf.vehicle_type,
    vf.model_year,
    vf.capacity_kg,
    vf.fuel_efficiency_mpg,
    vf.maintenance_interval_miles,
    vf.current_mileage,
    vf.vehicle_status,
    vf.vehicle_age_years,
    vf.vehicle_type_numeric,
    
    -- Null route features
    NULL AS route_name,
    NULL AS route_type,
    NULL AS total_distance_km,
    NULL AS estimated_duration_minutes,
    NULL AS complexity_score,
    NULL AS traffic_density,
    NULL AS weather_risk,
    NULL AS haul_type,
    
    -- Performance features
    COALESCE(vpf.daily_deliveries, 0) AS daily_deliveries,
    COALESCE(vpf.daily_distance_km, 0) AS daily_distance_km,
    COALESCE(vpf.daily_fuel_cost, 0) AS daily_fuel_cost,
    COALESCE(vpf.daily_on_time_rate, 0) AS daily_on_time_rate,
    COALESCE(vpf.daily_avg_rating, 0) AS daily_avg_rating,
    COALESCE(vpf.deliveries_7d_avg, 0) AS deliveries_7d_avg,
    COALESCE(vpf.deliveries_30d_avg, 0) AS deliveries_30d_avg,
    COALESCE(vpf.on_time_rate_7d_avg, 0) AS on_time_rate_7d_avg,
    COALESCE(vpf.on_time_rate_30d_avg, 0) AS on_time_rate_30d_avg,
    COALESCE(vpf.fuel_efficiency_7d_avg, 0) AS fuel_efficiency_7d_avg,
    COALESCE(vpf.fuel_efficiency_30d_avg, 0) AS fuel_efficiency_30d_avg,
    COALESCE(vpf.performance_trend_7d_vs_30d, 'stable') AS performance_trend_7d_vs_30d,
    COALESCE(vpf.fuel_efficiency_trend_7d_vs_30d, 'stable') AS fuel_efficiency_trend_7d_vs_30d,
    
    -- Null route performance features
    NULL AS daily_trips,
    NULL AS avg_customer_satisfaction,
    NULL AS total_fuel_cost,
    NULL AS total_revenue,
    NULL AS avg_fuel_cost_per_km,
    NULL AS satisfaction_7d_avg,
    NULL AS satisfaction_30d_avg,
    NULL AS route_performance_rating,
    NULL AS route_risk_level,
    
    -- Maintenance features
    COALESCE(mf.avg_engine_temp, 0) AS avg_engine_temp,
    COALESCE(mf.avg_engine_health, 0) AS avg_engine_health,
    COALESCE(mf.total_harsh_braking, 0) AS total_harsh_braking,
    COALESCE(mf.total_harsh_acceleration, 0) AS total_harsh_acceleration,
    COALESCE(mf.maintenance_events_30d, 0) AS maintenance_events_30d,
    COALESCE(mf.maintenance_cost_30d, 0) AS maintenance_cost_30d,
    COALESCE(mf.maintenance_urgency, 'LOW') AS maintenance_urgency,
    COALESCE(mf.days_since_last_maintenance, 0) AS days_since_last_maintenance,
    COALESCE(mf.miles_since_last_maintenance, 0) AS miles_since_last_maintenance,
    COALESCE(mf.predictive_maintenance_score, 0) AS predictive_maintenance_score,
    COALESCE(mf.maintenance_risk_score, 0) AS maintenance_risk_score,
    COALESCE(mf.next_maintenance_due_days, 0) AS next_maintenance_due_days,
    
    -- Null real-time features
    NULL AS shipment_id,
    NULL AS planned_delivery_date,
    NULL AS delivery_status,
    NULL AS current_hour,
    NULL AS current_dow

FROM vehicle_features vf
LEFT JOIN vehicle_performance_features vpf ON vf.vehicle_id = vpf.vehicle_id
LEFT JOIN maintenance_features mf ON vf.vehicle_id = mf.vehicle_id AND vpf.feature_date = mf.feature_date

UNION ALL

-- Route feature store
SELECT 
    'route' AS entity_type,
    rf.route_id AS entity_id,
    COALESCE(rpf.feature_date, CURRENT_DATE()) AS feature_date,
    
    -- Null customer features
    NULL AS customer_tier,
    NULL AS segment,
    NULL AS credit_limit_usd,
    NULL AS payment_terms,
    NULL AS customer_since_date,
    NULL AS customer_tenure_days,
    NULL AS customer_tier_numeric,
    
    -- Null behavioral features
    NULL AS daily_shipments,
    NULL AS daily_revenue,
    NULL AS shipments_30d_avg,
    NULL AS revenue_30d_avg,
    NULL AS shipments_7d_avg,
    NULL AS shipments_90d_avg,
    NULL AS activity_ratio_7d_vs_90d,
    NULL AS revenue_yoy_growth_percent,
    NULL AS shipment_volatility_30d,
    NULL AS behavior_consistency,
    
    -- Null vehicle features
    NULL AS vehicle_type,
    NULL AS model_year,
    NULL AS capacity_kg,
    NULL AS fuel_efficiency_mpg,
    NULL AS maintenance_interval_miles,
    NULL AS current_mileage,
    NULL AS vehicle_status,
    NULL AS vehicle_age_years,
    NULL AS vehicle_type_numeric,
    
    -- Route features
    rf.route_name,
    rf.route_type,
    rf.total_distance_km,
    rf.estimated_duration_minutes,
    rf.complexity_score,
    rf.traffic_density,
    rf.weather_risk,
    rf.haul_type,
    
    -- Null vehicle performance features
    NULL AS daily_deliveries,
    NULL AS daily_distance_km,
    NULL AS daily_fuel_cost,
    NULL AS daily_on_time_rate,
    NULL AS daily_avg_rating,
    NULL AS deliveries_7d_avg,
    NULL AS deliveries_30d_avg,
    NULL AS on_time_rate_7d_avg,
    NULL AS on_time_rate_30d_avg,
    NULL AS fuel_efficiency_7d_avg,
    NULL AS fuel_efficiency_30d_avg,
    NULL AS performance_trend_7d_vs_30d,
    NULL AS fuel_efficiency_trend_7d_vs_30d,
    
    -- Route performance features
    COALESCE(rpf.daily_trips, 0) AS daily_trips,
    COALESCE(rpf.daily_on_time_rate, 0) AS avg_customer_satisfaction,
    COALESCE(rpf.total_fuel_cost, 0) AS total_fuel_cost,
    COALESCE(rpf.total_revenue, 0) AS total_revenue,
    COALESCE(rpf.avg_fuel_cost_per_km, 0) AS avg_fuel_cost_per_km,
    COALESCE(rpf.on_time_rate_7d_avg, 0) AS satisfaction_7d_avg,
    COALESCE(rpf.on_time_rate_30d_avg, 0) AS satisfaction_30d_avg,
    COALESCE(rpf.route_performance_rating, 'unknown') AS route_performance_rating,
    COALESCE(rpf.route_risk_level, 'unknown') AS route_risk_level,
    
    -- Null maintenance features
    NULL AS avg_engine_temp,
    NULL AS avg_engine_health,
    NULL AS total_harsh_braking,
    NULL AS total_harsh_acceleration,
    NULL AS maintenance_events_30d,
    NULL AS maintenance_cost_30d,
    NULL AS maintenance_urgency,
    NULL AS days_since_last_maintenance,
    NULL AS miles_since_last_maintenance,
    NULL AS predictive_maintenance_score,
    NULL AS maintenance_risk_score,
    NULL AS next_maintenance_due_days,
    
    -- Null real-time features
    NULL AS shipment_id,
    NULL AS planned_delivery_date,
    NULL AS delivery_status,
    NULL AS current_hour,
    NULL AS current_dow

FROM route_features rf
LEFT JOIN route_performance_features rpf ON rf.route_id = rpf.route_id

UNION ALL

-- Real-time shipment features
SELECT 
    'shipment' AS entity_type,
    csf.shipment_id AS entity_id,
    csf.shipment_date AS feature_date,
    
    -- Null customer features
    NULL AS customer_tier,
    NULL AS segment,
    NULL AS credit_limit_usd,
    NULL AS payment_terms,
    NULL AS customer_since_date,
    NULL AS customer_tenure_days,
    NULL AS customer_tier_numeric,
    
    -- Null behavioral features
    NULL AS daily_shipments,
    NULL AS daily_revenue,
    NULL AS shipments_30d_avg,
    NULL AS revenue_30d_avg,
    NULL AS shipments_7d_avg,
    NULL AS shipments_90d_avg,
    NULL AS activity_ratio_7d_vs_90d,
    NULL AS revenue_yoy_growth_percent,
    NULL AS shipment_volatility_30d,
    NULL AS behavior_consistency,
    
    -- Vehicle features from shipment
    csf.vehicle_type,
    NULL AS model_year,
    NULL AS capacity_kg,
    NULL AS fuel_efficiency_mpg,
    NULL AS maintenance_interval_miles,
    NULL AS current_mileage,
    NULL AS vehicle_status,
    NULL AS vehicle_age_years,
    NULL AS vehicle_type_numeric,
    
    -- Route features from shipment
    NULL AS route_name,
    NULL AS route_type,
    csf.total_distance_km,
    NULL AS estimated_duration_minutes,
    csf.complexity_score,
    NULL AS traffic_density,
    NULL AS weather_risk,
    csf.haul_type,
    
    -- Null performance features
    NULL AS daily_deliveries,
    NULL AS daily_distance_km,
    NULL AS daily_fuel_cost,
    NULL AS daily_on_time_rate,
    NULL AS daily_avg_rating,
    NULL AS deliveries_7d_avg,
    NULL AS deliveries_30d_avg,
    NULL AS on_time_rate_7d_avg,
    NULL AS on_time_rate_30d_avg,
    NULL AS fuel_efficiency_7d_avg,
    NULL AS fuel_efficiency_30d_avg,
    NULL AS performance_trend_7d_vs_30d,
    NULL AS fuel_efficiency_trend_7d_vs_30d,
    NULL AS daily_trips,
    NULL AS avg_customer_satisfaction,
    NULL AS total_fuel_cost,
    NULL AS total_revenue,
    NULL AS avg_fuel_cost_per_km,
    NULL AS satisfaction_7d_avg,
    NULL AS satisfaction_30d_avg,
    NULL AS route_performance_rating,
    NULL AS route_risk_level,
    
    -- Null maintenance features
    NULL AS avg_engine_temp,
    NULL AS avg_engine_health,
    NULL AS total_harsh_braking,
    NULL AS total_harsh_acceleration,
    NULL AS maintenance_events_30d,
    NULL AS maintenance_cost_30d,
    NULL AS maintenance_urgency,
    NULL AS days_since_last_maintenance,
    NULL AS miles_since_last_maintenance,
    NULL AS predictive_maintenance_score,
    NULL AS maintenance_risk_score,
    NULL AS next_maintenance_due_days,
    
    -- Real-time features
    csf.shipment_id,
    csf.planned_delivery_date,
    csf.delivery_status,
    csf.current_hour,
    csf.current_dow

FROM current_shipment_features csf
