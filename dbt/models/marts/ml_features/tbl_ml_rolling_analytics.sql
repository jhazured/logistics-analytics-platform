-- =====================================================
-- Consolidated Rolling Analytics Model
-- Combines customer, operational, and route rolling analytics
-- =====================================================

{{ config(
    materialized='table',
    cluster_by=['entity_type', 'entity_id', 'feature_date'],
    tags=['ml', 'analytics', 'rolling', 'consolidated', 'ml_optimized']
) }}

WITH customer_daily_activity AS (
    SELECT 
        fs.customer_id,
        fs.shipment_date,
        dc.customer_name,
        dc.volume_segment,
        dc.customer_type,
        
        -- Daily customer metrics
        COUNT(*) AS daily_shipments,
        SUM(fs.weight_kg) AS daily_weight,
        SUM(fs.volume_m3) AS daily_volume,
        SUM(fs.revenue) AS daily_revenue,
        AVG(fs.route_efficiency_score) AS daily_avg_rating,
        {{ calculate_on_time_rate('fs.is_on_time') }} AS daily_on_time_rate,
        COUNT(DISTINCT fs.destination_location_id) AS daily_unique_destinations
        
    FROM {{ ref('tbl_fact_shipments') }} fs
    JOIN {{ ref('tbl_dim_customer') }} dc ON fs.customer_id = dc.customer_id
    WHERE fs.shipment_date >= CURRENT_DATE() - 730  -- 2 years of data
    GROUP BY 1, 2, 3, 4, 5
),

vehicle_daily_performance AS (
    SELECT 
        fs.vehicle_id,
        fs.shipment_date,
        dv.vehicle_type,
        dl_origin.city AS origin_city,
        
        -- Daily aggregations
        COUNT(*) AS daily_deliveries,
        SUM(fs.distance_km) AS daily_distance_km,
        SUM(fs.fuel_cost) AS daily_fuel_cost,
        SUM(fs.delivery_cost) AS daily_delivery_cost,
        SUM(fs.revenue) AS daily_revenue,
        AVG(fs.customer_rating) AS daily_avg_rating,
        {{ calculate_on_time_rate('fs.is_on_time') }} AS daily_on_time_rate,
        
        -- Efficiency metrics
        {{ calculate_speed_kmh('SUM(fs.distance_km)', 'SUM(fs.actual_duration_minutes)') }} AS daily_avg_speed_kmh,
        {{ calculate_cost_per_km('SUM(fs.fuel_cost)', 'SUM(fs.distance_km)') }} AS daily_fuel_cost_per_km,
        SUM(fs.revenue - fs.delivery_cost - fs.fuel_cost) AS daily_profit
        
    FROM {{ ref('tbl_fact_shipments') }} fs
    JOIN {{ ref('tbl_dim_vehicle') }} dv ON fs.vehicle_id = dv.vehicle_id
    JOIN {{ ref('tbl_dim_location') }} dl_origin ON fs.origin_location_id = dl_origin.location_id
    WHERE fs.is_delivered = TRUE
        AND fs.shipment_date >= CURRENT_DATE() - 365
    GROUP BY 1, 2, 3, 4
),

route_daily_performance AS (
    SELECT 
        fs.route_id,
        fs.shipment_date,
        dr.route_name,
        dr.route_type,
        {{ classify_haul_type('dr.distance_km') }} AS haul_type,
        dl_origin.city AS origin_city,
        
        -- Daily route metrics
        COUNT(*) AS daily_trips,
        AVG(fs.actual_duration_minutes) AS avg_actual_duration,
        AVG(fs.planned_duration_minutes) AS avg_planned_duration,
        AVG({{ safe_divide('fs.actual_duration_minutes', 'fs.planned_duration_minutes', 1) }}) AS avg_duration_ratio,
        {{ calculate_on_time_rate('fs.is_on_time') }} AS daily_on_time_rate,
        AVG(fs.customer_rating) AS avg_customer_satisfaction,
        SUM(fs.fuel_cost) AS total_fuel_cost,
        SUM(fs.delivery_cost) AS total_delivery_cost,
        SUM(fs.revenue) AS total_revenue,
        AVG({{ calculate_cost_per_km('fs.fuel_cost', 'fs.distance_km') }}) AS avg_fuel_cost_per_km,
        
        -- Operational challenges
        SUM(CASE WHEN fs.actual_duration_minutes > fs.planned_duration_minutes * 1.5 THEN 1 ELSE 0 END) AS severe_delays,
        SUM(CASE WHEN fs.customer_rating < 7 THEN 1 ELSE 0 END) AS poor_ratings
        
    FROM {{ ref('tbl_fact_shipments') }} fs
    JOIN {{ ref('tbl_dim_route') }} dr ON fs.route_id = dr.route_id
    JOIN {{ ref('tbl_dim_location') }} dl_origin ON fs.origin_location_id = dl_origin.location_id
    WHERE fs.shipment_date >= CURRENT_DATE() - 365
        AND fs.is_delivered = TRUE
    GROUP BY 1,2,3,4,5,6
),

-- Customer rolling analytics
customer_rolling AS (
    SELECT 
        'customer' AS entity_type,
        cda.customer_id AS entity_id,
        cda.shipment_date AS feature_date,
        cda.customer_name,
        cda.volume_segment,
        cda.customer_type,
        
        -- Current day metrics
        cda.daily_shipments,
        cda.daily_weight,
        cda.daily_volume,
        cda.daily_revenue,
        cda.daily_avg_rating,
        cda.daily_on_time_rate,
        
        -- 30-day rolling volumes
        AVG(cda.daily_shipments) OVER (
            PARTITION BY cda.customer_id 
            ORDER BY cda.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS shipments_30d_avg,
        AVG(cda.daily_revenue) OVER (
            PARTITION BY cda.customer_id 
            ORDER BY cda.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS revenue_30d_avg,
        AVG(cda.daily_weight) OVER (
            PARTITION BY cda.customer_id 
            ORDER BY cda.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS weight_30d_avg,
        
        SUM(cda.daily_shipments) OVER (
            PARTITION BY cda.customer_id 
            ORDER BY cda.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS shipments_30d_total,
        
        SUM(cda.daily_revenue) OVER (
            PARTITION BY cda.customer_id 
            ORDER BY cda.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS revenue_30d_total,
        
        -- Seasonal adjustment factors (year-over-year comparison)
        LAG(cda.daily_shipments, 365) OVER (
            PARTITION BY cda.customer_id 
            ORDER BY cda.shipment_date
        ) AS shipments_same_day_last_year,
        
        LAG(cda.daily_revenue, 365) OVER (
            PARTITION BY cda.customer_id 
            ORDER BY cda.shipment_date
        ) AS revenue_same_day_last_year,
        
        -- Calculate year-over-year growth
        CASE 
            WHEN LAG(cda.daily_revenue, 365) OVER (PARTITION BY cda.customer_id ORDER BY cda.shipment_date) > 0
            THEN (cda.daily_revenue / LAG(cda.daily_revenue, 365) OVER (PARTITION BY cda.customer_id ORDER BY cda.shipment_date) - 1) * 100
            ELSE NULL
        END AS revenue_yoy_growth_percent,
        
        -- Customer behavior change detection
        AVG(cda.daily_shipments) OVER (
            PARTITION BY cda.customer_id 
            ORDER BY cda.shipment_date 
            {{ rolling_window_days(7) }}
        ) AS shipments_7d_avg,
        AVG(cda.daily_shipments) OVER (
            PARTITION BY cda.customer_id 
            ORDER BY cda.shipment_date 
            {{ rolling_window_days(90) }}
        ) AS shipments_90d_avg,
        
        -- Activity ratio (recent vs historical)
        CASE 
            WHEN AVG(cda.daily_shipments) OVER (PARTITION BY cda.customer_id ORDER BY cda.shipment_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) > 0 
            THEN AVG(cda.daily_shipments) OVER (PARTITION BY cda.customer_id ORDER BY cda.shipment_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) / 
                 AVG(cda.daily_shipments) OVER (PARTITION BY cda.customer_id ORDER BY cda.shipment_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
            ELSE NULL 
        END AS activity_ratio_7d_vs_90d,
        
        -- Customer lifecycle indicators
        {{ days_between('MIN(cda.shipment_date) OVER (PARTITION BY cda.customer_id)', 'cda.shipment_date') }} AS days_since_first_order,
        {{ days_between('cda.shipment_date', 'MAX(cda.shipment_date) OVER (PARTITION BY cda.customer_id)') }} AS days_to_last_order,
        
        -- Behavior consistency scoring
        STDDEV(cda.daily_shipments) OVER (
            PARTITION BY cda.customer_id 
            ORDER BY cda.shipment_date 
            {{ rolling_window_days(30) }}
        ) / NULLIF(AVG(cda.daily_shipments) OVER (
            PARTITION BY cda.customer_id 
            ORDER BY cda.shipment_date 
            {{ rolling_window_days(30) }}
        ), 0) AS shipment_volatility_30d,
        
        CASE 
            WHEN STDDEV(cda.daily_shipments) OVER (PARTITION BY cda.customer_id ORDER BY cda.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) / 
                 NULLIF(AVG(cda.daily_shipments) OVER (PARTITION BY cda.customer_id ORDER BY cda.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0) <= 0.3 THEN 'very_consistent'
            WHEN STDDEV(cda.daily_shipments) OVER (PARTITION BY cda.customer_id ORDER BY cda.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) / 
                 NULLIF(AVG(cda.daily_shipments) OVER (PARTITION BY cda.customer_id ORDER BY cda.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0) <= 0.6 THEN 'consistent'
            WHEN STDDEV(cda.daily_shipments) OVER (PARTITION BY cda.customer_id ORDER BY cda.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) / 
                 NULLIF(AVG(cda.daily_shipments) OVER (PARTITION BY cda.customer_id ORDER BY cda.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0) <= 1.0 THEN 'moderate'
            ELSE 'volatile'
        END AS behavior_consistency

    FROM customer_daily_activity cda
),

-- Vehicle rolling analytics
vehicle_rolling AS (
    SELECT 
        'vehicle' AS entity_type,
        vdp.vehicle_id AS entity_id,
        vdp.shipment_date AS feature_date,
        vdp.vehicle_type,
        vdp.origin_city,
        
        -- Current day metrics
        vdp.daily_deliveries,
        vdp.daily_distance_km,
        vdp.daily_fuel_cost,
        vdp.daily_delivery_cost,
        vdp.daily_revenue,
        vdp.daily_avg_rating,
        vdp.daily_on_time_rate,
        vdp.daily_avg_speed_kmh,
        vdp.daily_fuel_cost_per_km,
        vdp.daily_profit,
        
        -- 7-day rolling averages
        AVG(vdp.daily_deliveries) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(7) }}
        ) AS deliveries_7d_avg,
        AVG(vdp.daily_distance_km) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(7) }}
        ) AS distance_7d_avg,
        AVG(vdp.daily_on_time_rate) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(7) }}
        ) AS on_time_rate_7d_avg,
        AVG(vdp.daily_avg_rating) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(7) }}
        ) AS rating_7d_avg,
        AVG(vdp.daily_fuel_cost_per_km) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(7) }}
        ) AS fuel_efficiency_7d_avg,
        
        -- 30-day rolling averages
        AVG(vdp.daily_deliveries) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS deliveries_30d_avg,
        AVG(vdp.daily_distance_km) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS distance_30d_avg,
        AVG(vdp.daily_on_time_rate) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS on_time_rate_30d_avg,
        AVG(vdp.daily_avg_rating) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS rating_30d_avg,
        AVG(vdp.daily_fuel_cost_per_km) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS fuel_efficiency_30d_avg,
        
        -- 90-day rolling averages
        AVG(vdp.daily_deliveries) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(90) }}
        ) AS deliveries_90d_avg,
        AVG(vdp.daily_distance_km) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(90) }}
        ) AS distance_90d_avg,
        AVG(vdp.daily_on_time_rate) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(90) }}
        ) AS on_time_rate_90d_avg,
        AVG(vdp.daily_avg_rating) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(90) }}
        ) AS rating_90d_avg,
        AVG(vdp.daily_fuel_cost_per_km) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(90) }}
        ) AS fuel_efficiency_90d_avg,
        
        -- Volatility measures
        STDDEV(vdp.daily_on_time_rate) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS on_time_volatility_30d,
        STDDEV(vdp.daily_fuel_cost_per_km) OVER (
            PARTITION BY vdp.vehicle_id 
            ORDER BY vdp.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS fuel_efficiency_volatility_30d,
        
        -- Performance degradation indicators
        CASE 
            WHEN AVG(vdp.daily_on_time_rate) OVER (PARTITION BY vdp.vehicle_id ORDER BY vdp.shipment_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) < 
                 AVG(vdp.daily_on_time_rate) OVER (PARTITION BY vdp.vehicle_id ORDER BY vdp.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) * 0.9 THEN 'degrading'
            WHEN AVG(vdp.daily_on_time_rate) OVER (PARTITION BY vdp.vehicle_id ORDER BY vdp.shipment_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) > 
                 AVG(vdp.daily_on_time_rate) OVER (PARTITION BY vdp.vehicle_id ORDER BY vdp.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) * 1.1 THEN 'improving'
            ELSE 'stable'
        END AS performance_trend_7d_vs_30d,
        
        CASE 
            WHEN AVG(vdp.daily_fuel_cost_per_km) OVER (PARTITION BY vdp.vehicle_id ORDER BY vdp.shipment_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) > 
                 AVG(vdp.daily_fuel_cost_per_km) OVER (PARTITION BY vdp.vehicle_id ORDER BY vdp.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) * 1.1 THEN 'degrading'
            WHEN AVG(vdp.daily_fuel_cost_per_km) OVER (PARTITION BY vdp.vehicle_id ORDER BY vdp.shipment_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) < 
                 AVG(vdp.daily_fuel_cost_per_km) OVER (PARTITION BY vdp.vehicle_id ORDER BY vdp.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) * 0.9 THEN 'improving'
            ELSE 'stable'
        END AS fuel_efficiency_trend_7d_vs_30d

    FROM vehicle_daily_performance vdp
),

-- Route rolling analytics
route_rolling AS (
    SELECT 
        'route' AS entity_type,
        rdp.route_id AS entity_id,
        rdp.shipment_date AS feature_date,
        rdp.route_name,
        rdp.route_type,
        rdp.haul_type,
        rdp.origin_city,
        
        -- Current performance
        rdp.daily_trips,
        rdp.avg_actual_duration,
        rdp.avg_planned_duration,
        rdp.avg_duration_ratio,
        rdp.daily_on_time_rate,
        rdp.avg_customer_satisfaction,
        rdp.total_fuel_cost,
        rdp.total_revenue,
        rdp.avg_fuel_cost_per_km,
        rdp.severe_delays,
        rdp.poor_ratings,
        
        -- 7-day rolling metrics
        AVG(rdp.daily_on_time_rate) OVER (
            PARTITION BY rdp.route_id 
            ORDER BY rdp.shipment_date 
            {{ rolling_window_days(7) }}
        ) AS on_time_rate_7d_avg,
        AVG(rdp.avg_duration_ratio) OVER (
            PARTITION BY rdp.route_id 
            ORDER BY rdp.shipment_date 
            {{ rolling_window_days(7) }}
        ) AS duration_ratio_7d_avg,
        AVG(rdp.avg_customer_satisfaction) OVER (
            PARTITION BY rdp.route_id 
            ORDER BY rdp.shipment_date 
            {{ rolling_window_days(7) }}
        ) AS satisfaction_7d_avg,
        AVG(rdp.avg_fuel_cost_per_km) OVER (
            PARTITION BY rdp.route_id 
            ORDER BY rdp.shipment_date 
            {{ rolling_window_days(7) }}
        ) AS fuel_efficiency_7d_avg,
        
        -- 30-day rolling metrics  
        AVG(rdp.daily_on_time_rate) OVER (
            PARTITION BY rdp.route_id 
            ORDER BY rdp.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS on_time_rate_30d_avg,
        AVG(rdp.avg_duration_ratio) OVER (
            PARTITION BY rdp.route_id 
            ORDER BY rdp.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS duration_ratio_30d_avg,
        AVG(rdp.avg_customer_satisfaction) OVER (
            PARTITION BY rdp.route_id 
            ORDER BY rdp.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS satisfaction_30d_avg,
        
        -- 90-day rolling metrics
        AVG(rdp.daily_on_time_rate) OVER (
            PARTITION BY rdp.route_id 
            ORDER BY rdp.shipment_date 
            {{ rolling_window_days(90) }}
        ) AS on_time_rate_90d_avg,
        AVG(rdp.avg_duration_ratio) OVER (
            PARTITION BY rdp.route_id 
            ORDER BY rdp.shipment_date 
            {{ rolling_window_days(90) }}
        ) AS duration_ratio_90d_avg,
        AVG(rdp.avg_customer_satisfaction) OVER (
            PARTITION BY rdp.route_id 
            ORDER BY rdp.shipment_date 
            {{ rolling_window_days(90) }}
        ) AS satisfaction_90d_avg,
        
        -- Volatility measures
        STDDEV(rdp.daily_on_time_rate) OVER (
            PARTITION BY rdp.route_id 
            ORDER BY rdp.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS on_time_volatility_30d,
        STDDEV(rdp.avg_duration_ratio) OVER (
            PARTITION BY rdp.route_id 
            ORDER BY rdp.shipment_date 
            {{ rolling_window_days(30) }}
        ) AS duration_volatility_30d,
        
        -- Performance classification
        CASE 
            WHEN AVG(rdp.daily_on_time_rate) OVER (PARTITION BY rdp.route_id ORDER BY rdp.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) >= 0.95 
                 AND AVG(rdp.avg_customer_satisfaction) OVER (PARTITION BY rdp.route_id ORDER BY rdp.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) >= 8.5 THEN 'excellent'
            WHEN AVG(rdp.daily_on_time_rate) OVER (PARTITION BY rdp.route_id ORDER BY rdp.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) >= 0.90 
                 AND AVG(rdp.avg_customer_satisfaction) OVER (PARTITION BY rdp.route_id ORDER BY rdp.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) >= 8.0 THEN 'good'
            WHEN AVG(rdp.daily_on_time_rate) OVER (PARTITION BY rdp.route_id ORDER BY rdp.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) >= 0.80 
                 AND AVG(rdp.avg_customer_satisfaction) OVER (PARTITION BY rdp.route_id ORDER BY rdp.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) >= 7.0 THEN 'acceptable'
            ELSE 'needs_improvement'
        END AS route_performance_rating,
        
        -- Risk assessment
        CASE 
            WHEN STDDEV(rdp.daily_on_time_rate) OVER (PARTITION BY rdp.route_id ORDER BY rdp.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) > 0.3 
                 OR STDDEV(rdp.avg_duration_ratio) OVER (PARTITION BY rdp.route_id ORDER BY rdp.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) > 0.4 THEN 'high_risk'
            WHEN STDDEV(rdp.daily_on_time_rate) OVER (PARTITION BY rdp.route_id ORDER BY rdp.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) > 0.2 
                 OR STDDEV(rdp.avg_duration_ratio) OVER (PARTITION BY rdp.route_id ORDER BY rdp.shipment_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) > 0.25 THEN 'medium_risk'
            ELSE 'low_risk'
        END AS route_risk_level

    FROM route_daily_performance rdp
)

-- Union all rolling analytics
SELECT * FROM customer_rolling
UNION ALL
SELECT * FROM vehicle_rolling
UNION ALL
SELECT * FROM route_rolling
