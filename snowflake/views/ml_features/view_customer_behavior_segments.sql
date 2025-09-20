-- Customer clustering for personalized delivery predictions
CREATE OR REPLACE VIEW ANALYTICS.view_customer_behavior_segments AS
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.customer_tier,
        c.segment,
        c.credit_limit_usd,
        c.customer_since_date,
        
        -- Volume metrics (last 90 days)
        COUNT(s.shipment_id) as total_shipments_90d,
        COUNT(s.shipment_id) / 3.0 as avg_monthly_volume,
        SUM(s.revenue) as total_revenue_90d,
        AVG(s.revenue) as avg_revenue_per_shipment,
        
        -- Delivery behavior metrics
        AVG(CASE WHEN s.delivery_status = 'DELIVERED' AND s.actual_delivery_date <= s.planned_delivery_date THEN 1 ELSE 0 END) as on_time_rate,
        AVG(s.customer_rating) as avg_customer_rating,
        STDDEV(s.customer_rating) as rating_consistency,
        
        -- Service preferences
        COUNT(CASE WHEN s.service_type = 'EXPRESS' THEN 1 END) as express_shipments,
        COUNT(CASE WHEN s.service_type = 'STANDARD' THEN 1 END) as standard_shipments,
        COUNT(CASE WHEN s.service_type = 'ECONOMY' THEN 1 END) as economy_shipments,
        
        -- Timing patterns
        AVG(EXTRACT(HOUR FROM s.shipment_date)) as avg_shipment_hour,
        COUNT(CASE WHEN EXTRACT(DOW FROM s.shipment_date) IN (1,7) THEN 1 END) as weekend_shipments,
        
        -- Cost sensitivity
        AVG(s.total_cost) as avg_cost_per_shipment,
        AVG(s.profit_margin_pct) as avg_profit_margin
        
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN {{ ref('fact_shipments') }} s ON c.customer_id = s.customer_id
    WHERE s.shipment_date >= CURRENT_DATE() - 90
        AND c.is_active = true
    GROUP BY c.customer_id, c.customer_name, c.customer_tier, c.segment, c.credit_limit_usd, c.customer_since_date
),
customer_segments AS (
    SELECT 
        customer_id,
        customer_name,
        customer_tier,
        segment,
        credit_limit_usd,
        customer_since_date,
        total_shipments_90d,
        avg_monthly_volume,
        total_revenue_90d,
        avg_revenue_per_shipment,
        on_time_rate,
        avg_customer_rating,
        rating_consistency,
        express_shipments,
        standard_shipments,
        economy_shipments,
        avg_shipment_hour,
        weekend_shipments,
        avg_cost_per_shipment,
        avg_profit_margin,
        
        -- Volume segmentation
        CASE 
            WHEN avg_monthly_volume >= 100 THEN 'high_volume'
            WHEN avg_monthly_volume >= 20 THEN 'medium_volume'
            ELSE 'low_volume'
        END AS volume_segment,
        
        -- Delivery flexibility score (0-100)
        CASE 
            WHEN on_time_rate > 0.95 AND rating_consistency < 0.5 THEN 100
            WHEN on_time_rate > 0.85 AND rating_consistency < 1.0 THEN 80
            WHEN on_time_rate > 0.70 AND rating_consistency < 1.5 THEN 60
            WHEN on_time_rate > 0.50 THEN 40
            ELSE 20
        END AS delivery_flexibility_score,
        
        -- Premium service preference (0-100)
        CASE 
            WHEN express_shipments::FLOAT / NULLIF(total_shipments_90d, 0) > 0.5 THEN 100
            WHEN express_shipments::FLOAT / NULLIF(total_shipments_90d, 0) > 0.3 THEN 80
            WHEN express_shipments::FLOAT / NULLIF(total_shipments_90d, 0) > 0.1 THEN 60
            WHEN standard_shipments::FLOAT / NULLIF(total_shipments_90d, 0) > 0.7 THEN 40
            ELSE 20
        END AS premium_service_preference,
        
        -- Delivery window adherence (0-100)
        CASE 
            WHEN on_time_rate > 0.95 THEN 100
            WHEN on_time_rate > 0.85 THEN 80
            WHEN on_time_rate > 0.70 THEN 60
            WHEN on_time_rate > 0.50 THEN 40
            ELSE 20
        END AS delivery_window_adherence,
        
        -- Customer value score
        CASE 
            WHEN total_revenue_90d > 100000 AND avg_profit_margin > 15 THEN 'HIGH_VALUE'
            WHEN total_revenue_90d > 50000 AND avg_profit_margin > 10 THEN 'MEDIUM_VALUE'
            WHEN total_revenue_90d > 10000 THEN 'LOW_VALUE'
            ELSE 'MINIMAL_VALUE'
        END AS customer_value_segment,
        
        -- Behavior pattern
        CASE 
            WHEN weekend_shipments::FLOAT / NULLIF(total_shipments_90d, 0) > 0.3 THEN 'WEEKEND_HEAVY'
            WHEN avg_shipment_hour < 9 OR avg_shipment_hour > 17 THEN 'OFF_HOURS'
            WHEN avg_shipment_hour BETWEEN 9 AND 17 THEN 'BUSINESS_HOURS'
            ELSE 'MIXED'
        END AS timing_pattern,
        
        -- Risk assessment
        CASE 
            WHEN on_time_rate < 0.5 OR avg_customer_rating < 3.0 THEN 'HIGH_RISK'
            WHEN on_time_rate < 0.7 OR avg_customer_rating < 4.0 THEN 'MEDIUM_RISK'
            ELSE 'LOW_RISK'
        END AS churn_risk_level
        
    FROM customer_metrics
)
SELECT 
    customer_id,
    customer_name,
    customer_tier,
    segment,
    credit_limit_usd,
    customer_since_date,
    total_shipments_90d,
    avg_monthly_volume,
    total_revenue_90d,
    avg_revenue_per_shipment,
    on_time_rate,
    avg_customer_rating,
    rating_consistency,
    express_shipments,
    standard_shipments,
    economy_shipments,
    avg_shipment_hour,
    weekend_shipments,
    avg_cost_per_shipment,
    avg_profit_margin,
    
    -- Segmentation results
    volume_segment,
    delivery_flexibility_score,
    premium_service_preference,
    delivery_window_adherence,
    customer_value_segment,
    timing_pattern,
    churn_risk_level,
    
    -- ML features for clustering
    (delivery_flexibility_score + premium_service_preference + delivery_window_adherence) / 3.0 as overall_behavior_score,
    
    -- Customer lifecycle stage
    CASE 
        WHEN DATEDIFF('day', customer_since_date, CURRENT_DATE()) < 90 THEN 'NEW'
        WHEN DATEDIFF('day', customer_since_date, CURRENT_DATE()) < 365 THEN 'GROWING'
        WHEN DATEDIFF('day', customer_since_date, CURRENT_DATE()) < 1095 THEN 'ESTABLISHED'
        ELSE 'MATURE'
    END AS lifecycle_stage,
    
    CURRENT_TIMESTAMP() as segment_updated_at

FROM customer_segments
WHERE total_shipments_90d > 0
ORDER BY total_revenue_90d DESC