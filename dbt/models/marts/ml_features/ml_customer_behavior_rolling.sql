-- =====================================================
-- Rolling Window Analytics Views
-- =====================================================

-- Customer Behavior Rolling
{{ config(
    materialized='table',
    tags=['analytics', 'rolling', 'customer']
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
        AVG(fs.customer_rating) AS daily_avg_rating,
        AVG(CASE WHEN fs.is_on_time THEN 1.0 ELSE 0.0 END) AS daily_on_time_rate,
        COUNT(DISTINCT fs.destination_location_id) AS daily_unique_destinations
        
    FROM {{ ref('fact_shipments') }} fs
    JOIN {{ ref('dim_customer') }} dc ON fs.customer_id = dc.customer_id
    WHERE fs.shipment_date >= CURRENT_DATE() - 730  -- 2 years of data
    GROUP BY 1, 2, 3, 4, 5
)

SELECT 
    cda.customer_id,
    cda.shipment_date,
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
    {{ rolling_average('daily_shipments', 'customer_id', 'shipment_date', [30]) }} AS shipments_30d_avg,
    {{ rolling_average('daily_revenue', 'customer_id', 'shipment_date', [30]) }} AS revenue_30d_avg,
    {{ rolling_average('daily_weight', 'customer_id', 'shipment_date', [30]) }} AS weight_30d_avg,
    
    SUM(cda.daily_shipments) OVER (
        PARTITION BY cda.customer_id 
        ORDER BY cda.shipment_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS shipments_30d_total,
    
    SUM(cda.daily_revenue) OVER (
        PARTITION BY cda.customer_id 
        ORDER BY cda.shipment_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
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
    {{ rolling_average('daily_shipments', 'customer_id', 'shipment_date', [7]) }} AS shipments_7d_avg,
    {{ rolling_average('daily_shipments', 'customer_id', 'shipment_date', [90]) }} AS shipments_90d_avg,
    
    -- Activity ratio (recent vs historical)
    CASE 
        WHEN shipments_90d_avg > 0 
        THEN shipments_7d_avg / shipments_90d_avg 
        ELSE NULL 
    END AS activity_ratio_7d_vs_90d,
    
    -- Trend indicators
    {{ calculate_trend('shipments_7d_avg', 'shipments_30d_avg') }} AS volume_trend_7d_vs_30d,
    {{ calculate_trend('revenue_30d_avg', 'revenue_90d_avg') }} AS revenue_trend_30d_vs_90d,
    
    -- Customer lifecycle indicators
    DATEDIFF(day, MIN(cda.shipment_date) OVER (PARTITION BY cda.customer_id), cda.shipment_date) AS days_since_first_order,
    DATEDIFF(day, cda.shipment_date, MAX(cda.shipment_date) OVER (PARTITION BY cda.customer_id)) AS days_to_last_order,
    
    -- Behavior consistency scoring
    {{ calculate_volatility('daily_shipments', 'customer_id', 'shipment_date', 30) }} AS shipment_volatility_30d,
    
    CASE 
        WHEN shipment_volatility_30d <= 0.3 THEN 'very_consistent'
        WHEN shipment_volatility_30d <= 0.6 THEN 'consistent'
        WHEN shipment_volatility_30d <= 1.0 THEN 'moderate'
        ELSE 'volatile'
    END AS behavior_consistency

FROM customer_daily_activity cda