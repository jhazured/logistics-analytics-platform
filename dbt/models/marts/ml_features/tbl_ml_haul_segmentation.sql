-- =====================================================
-- ML-Ready Data Product Views
-- =====================================================

-- 1. Haul Segmentation Features
-- File: models/analytics/ml_features/view_haul_segmentation.sql
{{ config(
    materialized='table',
    tags=['ml', 'features', 'segmentation']
) }}

WITH shipment_base AS (
    SELECT 
        fs.*,
        dl_origin.city AS origin_city,
        dl_dest.city AS destination_city,
        dr.route_type,
        dr.complexity_score,
        dc.volume_segment,
        dc.service_level,
        dv.vehicle_type,
        dv.capacity_kg,
        dd.is_weekend,
        dd.season,
        dd.logistics_day_type
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
    {{ classify_haul_type('distance_km') }} AS haul_type,
    {{ classify_delivery_window('planned_delivery_date', 'actual_delivery_date') }} AS delivery_window,
    
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
    EXTRACT(hour FROM shipment_date) AS hour_of_day,
    EXTRACT(dayofweek FROM shipment_date) AS day_of_week,
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