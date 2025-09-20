-- Rolling customer behavior analytics view
-- Provides 7-day, 30-day, and 90-day rolling metrics for customer behavior analysis

CREATE OR REPLACE VIEW view_customer_behavior_rolling AS
WITH customer_shipments AS (
    SELECT 
        customer_id,
        shipment_date,
        revenue,
        delivery_status,
        customer_rating,
        is_on_time,
        priority_level,
        service_type
    FROM {{ ref('fact_shipments') }}
),

customer_metrics AS (
    SELECT 
        customer_id,
        shipment_date,
        -- Daily metrics
        COUNT(*) as daily_shipments,
        SUM(revenue) as daily_revenue,
        AVG(customer_rating) as daily_avg_rating,
        SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) as daily_on_time_deliveries,
        COUNT(*) as daily_total_deliveries,
        -- Service type distribution
        SUM(CASE WHEN service_type = 'EXPRESS' THEN 1 ELSE 0 END) as daily_express_shipments,
        SUM(CASE WHEN service_type = 'STANDARD' THEN 1 ELSE 0 END) as daily_standard_shipments,
        SUM(CASE WHEN service_type = 'OVERNIGHT' THEN 1 ELSE 0 END) as daily_overnight_shipments,
        -- Priority distribution
        SUM(CASE WHEN priority_level = 'HIGH' THEN 1 ELSE 0 END) as daily_high_priority_shipments
    FROM customer_shipments
    GROUP BY customer_id, shipment_date
)

SELECT 
    customer_id,
    shipment_date,
    daily_shipments,
    daily_revenue,
    daily_avg_rating,
    daily_on_time_deliveries,
    daily_total_deliveries,
    daily_express_shipments,
    daily_standard_shipments,
    daily_overnight_shipments,
    daily_high_priority_shipments,
    
    -- Rolling 7-day metrics
    AVG(daily_shipments) OVER (
        PARTITION BY customer_id 
        ORDER BY shipment_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7d_avg_shipments,
    
    SUM(daily_revenue) OVER (
        PARTITION BY customer_id 
        ORDER BY shipment_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7d_total_revenue,
    
    AVG(daily_avg_rating) OVER (
        PARTITION BY customer_id 
        ORDER BY shipment_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7d_avg_rating,
    
    SUM(daily_on_time_deliveries) OVER (
        PARTITION BY customer_id 
        ORDER BY shipment_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7d_on_time_deliveries,
    
    SUM(daily_total_deliveries) OVER (
        PARTITION BY customer_id 
        ORDER BY shipment_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7d_total_deliveries,
    
    -- Rolling 30-day metrics
    AVG(daily_shipments) OVER (
        PARTITION BY customer_id 
        ORDER BY shipment_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as rolling_30d_avg_shipments,
    
    SUM(daily_revenue) OVER (
        PARTITION BY customer_id 
        ORDER BY shipment_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as rolling_30d_total_revenue,
    
    AVG(daily_avg_rating) OVER (
        PARTITION BY customer_id 
        ORDER BY shipment_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as rolling_30d_avg_rating,
    
    SUM(daily_on_time_deliveries) OVER (
        PARTITION BY customer_id 
        ORDER BY shipment_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as rolling_30d_on_time_deliveries,
    
    SUM(daily_total_deliveries) OVER (
        PARTITION BY customer_id 
        ORDER BY shipment_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as rolling_30d_total_deliveries,
    
    -- Rolling 90-day metrics
    AVG(daily_shipments) OVER (
        PARTITION BY customer_id 
        ORDER BY shipment_date 
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) as rolling_90d_avg_shipments,
    
    SUM(daily_revenue) OVER (
        PARTITION BY customer_id 
        ORDER BY shipment_date 
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) as rolling_90d_total_revenue,
    
    AVG(daily_avg_rating) OVER (
        PARTITION BY customer_id 
        ORDER BY shipment_date 
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) as rolling_90d_avg_rating,
    
    SUM(daily_on_time_deliveries) OVER (
        PARTITION BY customer_id 
        ORDER BY shipment_date 
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) as rolling_90d_on_time_deliveries,
    
    SUM(daily_total_deliveries) OVER (
        PARTITION BY customer_id 
        ORDER BY shipment_date 
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) as rolling_90d_total_deliveries,
    
    -- Calculated metrics
    CASE 
        WHEN rolling_7d_total_deliveries > 0 
        THEN (rolling_7d_on_time_deliveries::FLOAT / rolling_7d_total_deliveries) * 100 
        ELSE 0 
    END as rolling_7d_on_time_percentage,
    
    CASE 
        WHEN rolling_30d_total_deliveries > 0 
        THEN (rolling_30d_on_time_deliveries::FLOAT / rolling_30d_total_deliveries) * 100 
        ELSE 0 
    END as rolling_30d_on_time_percentage,
    
    CASE 
        WHEN rolling_90d_total_deliveries > 0 
        THEN (rolling_90d_on_time_deliveries::FLOAT / rolling_90d_total_deliveries) * 100 
        ELSE 0 
    END as rolling_90d_on_time_percentage

FROM customer_metrics
ORDER BY customer_id, shipment_date;
