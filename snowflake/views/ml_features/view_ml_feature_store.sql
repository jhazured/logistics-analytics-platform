-- ML Feature Store View
-- This view provides a comprehensive feature store for machine learning models

CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.ANALYTICS.V_ML_FEATURE_STORE AS
WITH customer_features AS (
    SELECT 
        customer_id,
        customer_tier,
        industry_vertical,
        credit_limit_usd,
        payment_terms_days,
        customer_since_date,
        DATEDIFF('day', customer_since_date, CURRENT_DATE()) as customer_tenure_days,
        CASE 
            WHEN customer_tier = 'PREMIUM' THEN 3
            WHEN customer_tier = 'STANDARD' THEN 2
            WHEN customer_tier = 'BASIC' THEN 1
            ELSE 0
        END as customer_tier_numeric
    FROM LOGISTICS_DW_PROD.MARTS.DIM_CUSTOMER
    WHERE is_active = true
),

vehicle_features AS (
    SELECT 
        vehicle_id,
        vehicle_type,
        model_year,
        capacity_lbs,
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
    FROM LOGISTICS_DW_PROD.MARTS.DIM_VEHICLE
    WHERE vehicle_status = 'ACTIVE'
),

route_features AS (
    SELECT 
        route_id,
        origin_location_id,
        destination_location_id,
        distance_miles,
        estimated_travel_time_hours,
        route_type,
        CASE 
            WHEN route_type = 'HIGHWAY' THEN 1
            WHEN route_type = 'URBAN' THEN 2
            WHEN route_type = 'RURAL' THEN 3
            ELSE 4
        END as route_type_numeric
    FROM LOGISTICS_DW_PROD.MARTS.DIM_ROUTE
),

shipment_features AS (
    SELECT 
        shipment_id,
        customer_id,
        vehicle_id,
        route_id,
        shipment_date,
        actual_delivery_time_hours,
        estimated_delivery_time_hours,
        on_time_delivery_flag,
        revenue_usd,
        total_cost_usd,
        profit_margin_pct,
        route_efficiency_score,
        carbon_emissions_kg,
        weather_delay_minutes,
        traffic_delay_minutes,
        EXTRACT(HOUR FROM shipment_date) as shipment_hour,
        EXTRACT(DOW FROM shipment_date) as shipment_day_of_week,
        EXTRACT(MONTH FROM shipment_date) as shipment_month,
        CASE 
            WHEN EXTRACT(DOW FROM shipment_date) IN (1,7) THEN 1 ELSE 0 
        END as is_weekend,
        CASE 
            WHEN EXTRACT(HOUR FROM shipment_date) BETWEEN 6 AND 18 THEN 1 ELSE 0 
        END as is_business_hours,
        (actual_delivery_time_hours - estimated_delivery_time_hours) as delivery_time_variance_hours,
        CASE 
            WHEN actual_delivery_time_hours > estimated_delivery_time_hours * 1.2 THEN 1 ELSE 0 
        END as is_delayed
    FROM LOGISTICS_DW_PROD.MARTS.FACT_SHIPMENTS
    WHERE shipment_date >= DATEADD('day', -365, CURRENT_DATE())
)

SELECT 
    s.shipment_id,
    s.customer_id,
    s.vehicle_id,
    s.route_id,
    s.shipment_date,
    
    -- Customer features
    cf.customer_tier_numeric,
    cf.customer_tenure_days,
    cf.credit_limit_usd,
    cf.payment_terms_days,
    
    -- Vehicle features
    vf.vehicle_age_years,
    vf.vehicle_type_numeric,
    vf.capacity_lbs,
    vf.fuel_efficiency_mpg,
    vf.current_mileage,
    
    -- Route features
    rf.distance_miles,
    rf.estimated_travel_time_hours,
    rf.route_type_numeric,
    
    -- Shipment features
    s.actual_delivery_time_hours,
    s.estimated_delivery_time_hours,
    s.delivery_time_variance_hours,
    s.is_delayed,
    s.on_time_delivery_flag,
    s.revenue_usd,
    s.total_cost_usd,
    s.profit_margin_pct,
    s.route_efficiency_score,
    s.carbon_emissions_kg,
    s.weather_delay_minutes,
    s.traffic_delay_minutes,
    s.shipment_hour,
    s.shipment_day_of_week,
    s.shipment_month,
    s.is_weekend,
    s.is_business_hours,
    
    -- Derived features
    s.revenue_usd / NULLIF(s.actual_delivery_time_hours, 0) as revenue_per_hour,
    s.total_cost_usd / NULLIF(s.distance_miles, 0) as cost_per_mile,
    s.carbon_emissions_kg / NULLIF(s.distance_miles, 0) as emissions_per_mile,
    
    -- Rolling averages (last 30 days)
    AVG(s.on_time_delivery_flag) OVER (
        PARTITION BY s.customer_id 
        ORDER BY s.shipment_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as customer_on_time_rate_30d,
    
    AVG(s.route_efficiency_score) OVER (
        PARTITION BY s.route_id 
        ORDER BY s.shipment_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as route_efficiency_30d,
    
    AVG(s.profit_margin_pct) OVER (
        PARTITION BY s.vehicle_id 
        ORDER BY s.shipment_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as vehicle_profit_margin_30d,
    
    -- Feature engineering for ML models
    CASE 
        WHEN AVG(s.on_time_delivery_flag) OVER (
            PARTITION BY s.customer_id 
            ORDER BY s.shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) > 0.9 THEN 'HIGH_RELIABILITY'
        WHEN AVG(s.on_time_delivery_flag) OVER (
            PARTITION BY s.customer_id 
            ORDER BY s.shipment_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) > 0.7 THEN 'MEDIUM_RELIABILITY'
        ELSE 'LOW_RELIABILITY'
    END as customer_reliability_tier,
    
    -- Risk scoring
    (CASE WHEN s.is_delayed = 1 THEN 1 ELSE 0 END * 0.3 +
     CASE WHEN s.weather_delay_minutes > 60 THEN 1 ELSE 0 END * 0.2 +
     CASE WHEN s.traffic_delay_minutes > 30 THEN 1 ELSE 0 END * 0.2 +
     CASE WHEN vf.vehicle_age_years > 10 THEN 1 ELSE 0 END * 0.15 +
     CASE WHEN cf.customer_tier_numeric = 1 THEN 1 ELSE 0 END * 0.15) as risk_score,
     
    -- Feature hash for model versioning
    HASH(*) as feature_hash,
    CURRENT_TIMESTAMP() as feature_created_at

FROM shipment_features s
LEFT JOIN customer_features cf ON s.customer_id = cf.customer_id
LEFT JOIN vehicle_features vf ON s.vehicle_id = vf.vehicle_id
LEFT JOIN route_features rf ON s.route_id = rf.route_id
WHERE s.shipment_date >= DATEADD('day', -90, CURRENT_DATE())  -- Last 90 days for ML training
