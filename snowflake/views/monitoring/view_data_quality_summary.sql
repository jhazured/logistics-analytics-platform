CREATE OR REPLACE VIEW vw_data_quality_summary AS
SELECT 
    'dim_date' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT date_key) as unique_keys,
    SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END) as null_keys
FROM dim_date
UNION ALL
SELECT 
    'dim_customer' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT customer_id) as unique_keys,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_keys
FROM dim_customer
UNION ALL
SELECT 
    'fact_shipments' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT shipment_id) as unique_keys,
    SUM(CASE WHEN shipment_id IS NULL THEN 1 ELSE 0 END) as null_keys
FROM fact_shipments;