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
        actual_delivery_time_hours,
        on_time_delivery_flag,
        revenue_usd,
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
    
    -- Average delivery time
    SELECT 
        'avg_delivery_time_hours' as metric_name,
        AVG(actual_delivery_time_hours) as metric_value,
        OBJECT_CONSTRUCT('timeframe', 'last_hour') as dimensions
    FROM shipment_updates
    
    UNION ALL
    
    -- Revenue per hour
    SELECT 
        'revenue_per_hour' as metric_name,
        SUM(revenue_usd) as metric_value,
        OBJECT_CONSTRUCT('timeframe', 'current_hour') as dimensions
    FROM shipment_updates
)
SELECT * FROM real_time_metrics;

-- Enable the task
ALTER TASK process_shipment_updates RESUME;