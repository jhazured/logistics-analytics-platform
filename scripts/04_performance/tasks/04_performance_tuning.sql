-- Performance tuning and optimization scripts for Snowflake
-- This file contains various performance optimization strategies

-- 1. Warehouse optimization
-- Create optimized warehouses for different workloads
CREATE OR REPLACE WAREHOUSE COMPUTE_WH_ANALYTICS
WITH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Optimized warehouse for analytical workloads';

CREATE OR REPLACE WAREHOUSE COMPUTE_WH_ML
WITH
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 600
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Optimized warehouse for ML workloads';

-- 2. Clustering optimization
-- Add clustering keys to large fact tables for better query performance
ALTER TABLE fact_shipments CLUSTER BY (shipment_date, customer_id);
ALTER TABLE fact_vehicle_telemetry CLUSTER BY (timestamp, vehicle_id);
ALTER TABLE fact_route_performance CLUSTER BY (performance_date, route_id);

-- 3. Materialized view for frequently accessed aggregations
CREATE OR REPLACE MATERIALIZED VIEW mv_daily_shipment_summary
AS
SELECT 
    shipment_date,
    customer_id,
    COUNT(*) as total_shipments,
    SUM(revenue) as total_revenue,
    AVG(customer_rating) as avg_rating,
    SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) as on_time_deliveries,
    COUNT(*) as total_deliveries
FROM fact_shipments
GROUP BY shipment_date, customer_id;

-- 4. Query result caching optimization
-- Enable query result caching for frequently accessed data
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

-- 5. Search optimization
-- Add search optimization to frequently queried columns
ALTER TABLE fact_shipments ADD SEARCH OPTIMIZATION ON EQUALITY(customer_id, vehicle_id, route_id);
ALTER TABLE fact_vehicle_telemetry ADD SEARCH OPTIMIZATION ON EQUALITY(vehicle_id);

-- 6. Automatic clustering
-- Enable automatic clustering for large tables
ALTER TABLE fact_shipments SET AUTO_CLUSTERING = TRUE;
ALTER TABLE fact_vehicle_telemetry SET AUTO_CLUSTERING = TRUE;

-- 7. Query performance monitoring
-- Create a view to monitor query performance
CREATE OR REPLACE VIEW query_performance_monitor AS
SELECT 
    query_id,
    query_text,
    start_time,
    end_time,
    total_elapsed_time,
    warehouse_name,
    warehouse_size,
    credits_used_cloud_services,
    bytes_scanned,
    rows_produced,
    CASE 
        WHEN total_elapsed_time > 30000 THEN 'SLOW'
        WHEN total_elapsed_time > 10000 THEN 'MEDIUM'
        ELSE 'FAST'
    END as performance_category
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -7, CURRENT_DATE())
ORDER BY total_elapsed_time DESC;

-- 8. Resource monitor optimization
-- Create resource monitors to control costs
CREATE OR REPLACE RESOURCE MONITOR rm_analytics_warehouse
WITH
    CREDIT_QUOTA = 1000
    FREQUENCY = 'MONTHLY'
    START_TIMESTAMP = 'IMMEDIATE'
    TRIGGERS
        ON 80 PERCENT DO NOTIFY
        ON 90 PERCENT DO SUSPEND
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- 9. Table optimization recommendations
-- Create a view to identify tables that need optimization
CREATE OR REPLACE VIEW table_optimization_recommendations AS
SELECT 
    table_name,
    table_size_bytes,
    table_size_bytes / (1024*1024*1024) as table_size_gb,
    row_count,
    CASE 
        WHEN table_size_bytes > 1000000000 AND clustering_key IS NULL THEN 'ADD_CLUSTERING'
        WHEN table_size_bytes > 10000000000 THEN 'CONSIDER_PARTITIONING'
        WHEN row_count > 100000000 THEN 'ADD_SEARCH_OPTIMIZATION'
        ELSE 'NO_OPTIMIZATION_NEEDED'
    END as optimization_recommendation
FROM snowflake.information_schema.tables
WHERE table_schema = 'PUBLIC'
ORDER BY table_size_bytes DESC;

-- 10. Index optimization
-- Create indexes for frequently queried columns
CREATE OR REPLACE INDEX idx_shipments_customer_date ON fact_shipments(customer_id, shipment_date);
CREATE OR REPLACE INDEX idx_telemetry_vehicle_timestamp ON fact_vehicle_telemetry(vehicle_id, timestamp);

-- 11. Query optimization tips
-- Create a view with query optimization recommendations
CREATE OR REPLACE VIEW query_optimization_tips AS
SELECT 
    'Use WHERE clauses to filter data early' as tip_1,
    'Use LIMIT to reduce result set size' as tip_2,
    'Use appropriate data types to reduce storage' as tip_3,
    'Use clustering keys for large tables' as tip_4,
    'Use materialized views for repeated aggregations' as tip_5,
    'Use search optimization for equality predicates' as tip_6,
    'Use result caching for repeated queries' as tip_7,
    'Use appropriate warehouse sizes' as tip_8;

-- 12. Performance monitoring alerts
-- Create alerts for performance issues
CREATE OR REPLACE ALERT performance_alert
WAREHOUSE = COMPUTE_WH_SMALL
SCHEDULE = '5 MINUTE'
COMMENT = 'Alert for slow queries and high resource usage'
AS
SELECT 
    'SLOW_QUERY_ALERT' as alert_type,
    'Query taking longer than 30 seconds detected' as message
FROM snowflake.account_usage.query_history
WHERE total_elapsed_time > 30000
AND start_time >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
LIMIT 1;
