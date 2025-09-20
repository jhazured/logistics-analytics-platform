-- Create tasks for real-time processing
CREATE OR REPLACE TASK process_shipment_updates
WAREHOUSE = COMPUTE_WH_SMALL
SCHEDULE = '1 MINUTE'
COMMENT = 'Process real-time shipment updates'
AS
INSERT INTO real_time_kpis (metric_name, metric_value, dimensions)
WITH shipment_updates AS (
    SELECT 
        shipment_id,
        customer_id,
        -- Use actual columns from current schema
        planned_duration_minutes,
        actual_duration_minutes,
        is_on_time,
        on_time_delivery_flag,
        revenue,
        total_cost_usd,
        profit_margin_pct,
        route_efficiency_score,
        METADATA$ACTION as action_type,
        METADATA$ISUPDATE as is_update
    FROM shipments_stream
    WHERE METADATA$ACTION IN ('INSERT', 'UPDATE')
),
real_time_metrics AS (
    -- On-time delivery rate
    SELECT 
        'on_time_delivery_rate' as metric_name,
        AVG(on_time_delivery_flag::FLOAT) * 100 as metric_value,
        OBJECT_CONSTRUCT('timeframe', 'last_hour') as dimensions
    FROM shipment_updates
    
    UNION ALL
    
    -- Average delivery time (convert minutes to hours)
    SELECT 
        'avg_delivery_time_hours' as metric_name,
        AVG(actual_duration_minutes) / 60.0 as metric_value,
        OBJECT_CONSTRUCT('timeframe', 'last_hour') as dimensions
    FROM shipment_updates
    
    UNION ALL
    
    -- Revenue per hour
    SELECT 
        'revenue_per_hour' as metric_name,
        SUM(revenue) as metric_value,
        OBJECT_CONSTRUCT('timeframe', 'current_hour') as dimensions
    FROM shipment_updates
    
    UNION ALL
    
    -- Average profit margin
    SELECT 
        'avg_profit_margin' as metric_name,
        AVG(profit_margin_pct) as metric_value,
        OBJECT_CONSTRUCT('timeframe', 'last_hour') as dimensions
    FROM shipment_updates
    
    UNION ALL
    
    -- Average route efficiency
    SELECT 
        'avg_route_efficiency' as metric_name,
        AVG(route_efficiency_score) as metric_value,
        OBJECT_CONSTRUCT('timeframe', 'last_hour') as dimensions
    FROM shipment_updates
)
SELECT * FROM real_time_metrics;

-- Enable the task
ALTER TASK process_shipment_updates RESUME;