-- =====================================================
-- ML-Ready Data Product Views
-- =====================================================

-- 1. Haul Segmentation Features
-- File: models/analytics/ml_features/view_haul_segmentation.sql
{{ config(
    materialized='table',
    tags=['marts', 'ml_features', 'ml', 'features', 'segmentation', 'load_third']
) }}

WITH shipment_base AS (
    SELECT 
        -- Fact table columns (primary data source)
        fs.shipment_id,
        fs.shipment_date,
        fs.customer_id,
        fs.vehicle_id,
        fs.route_id,
        fs.origin_location_id,
        fs.destination_location_id,
        fs.distance_km,
        fs.weight_kg,
        fs.priority_level,
        fs.planned_duration_minutes,
        fs.actual_duration_minutes,
        fs.is_on_time,
        fs.route_efficiency_score as customer_rating,
        fs.fuel_cost,
        fs.delivery_cost,
        fs.revenue,
        fs.vehicle_type,
        
        -- Dimension context (for additional features)
        dl_origin.location_id AS origin_city,
        dl_dest.location_id AS destination_city,
        null as route_type,
        null as complexity_score,
        dc.customer_tier as volume_segment,
        dc.customer_type as service_level,
        dv.capacity_kg,
        dd.is_weekend,
        null as season,
        null as logistics_day_type,
        null as hour_of_day,
        dd.day_of_week
    FROM {{ ref('tbl_fact_shipments') }} fs
    JOIN {{ ref('tbl_dim_location') }} dl_origin ON fs.origin_location_id = dl_origin.location_id
    JOIN {{ ref('tbl_dim_location') }} dl_dest ON fs.destination_location_id = dl_dest.location_id
    JOIN {{ ref('tbl_dim_route') }} dr ON fs.route_id = dr.route_id
    JOIN {{ ref('tbl_dim_customer') }} dc ON fs.customer_id = dc.customer_id
    JOIN {{ ref('tbl_dim_vehicle') }} dv ON fs.vehicle_id = dv.vehicle_id
    JOIN {{ ref('tbl_dim_date') }} dd ON fs.date_key = dd.date_key
    WHERE fs.is_delivered = TRUE
)

SELECT 
    shipment_id,
    shipment_date,
    CASE 
        WHEN distance_km <= 50 THEN 'LOCAL'
        WHEN distance_km <= 200 THEN 'REGIONAL'
        WHEN distance_km <= 500 THEN 'LONG_HAUL'
        ELSE 'EXTREME_LONG_HAUL'
    END AS haul_type,
    null AS delivery_window,
    
    -- Distance and time features
    distance_km,
    planned_duration_minutes,
    actual_duration_minutes,
    CASE 
        WHEN planned_duration_minutes > 0 
        THEN actual_duration_minutes / planned_duration_minutes 
        ELSE NULL 
    END AS duration_ratio,
    
    -- Route complexity features
    route_type,
    complexity_score,
    CASE 
        WHEN complexity_score <= 3 THEN 'simple'
        WHEN complexity_score <= 7 THEN 'moderate'
        ELSE 'complex'
    END AS complexity_category,
    
    -- Customer and service features
    volume_segment,
    service_level,
    priority_level,
    
    -- Vehicle features
    vehicle_type,
    capacity_kg,
    weight_kg / NULLIF(capacity_kg, 0) AS capacity_utilization,
    
    -- Temporal features
    hour_of_day,
    day_of_week,
    is_weekend,
    season,
    logistics_day_type,
    
    -- Performance metrics
    is_on_time,
    customer_rating,
    fuel_cost,
    delivery_cost,
    revenue,
    revenue - delivery_cost - fuel_cost AS profit_margin,
    
    -- Route impact factors
    CASE 
        WHEN origin_city = destination_city THEN 'intra_city'
        ELSE 'inter_city'
    END AS route_scope

FROM shipment_base