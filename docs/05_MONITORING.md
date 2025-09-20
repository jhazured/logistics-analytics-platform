# Monitoring & Alerting

## Real-time Monitoring

### Data Quality Monitoring

Comprehensive data quality monitoring with automated alerting:

```sql
-- Data quality summary view
CREATE VIEW vw_data_quality_summary AS
SELECT 
  'tbl_dim_date' as table_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT date_key) as unique_keys,
  SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END) as null_keys
FROM tbl_dim_date
UNION ALL
SELECT 
  'tbl_dim_customer' as table_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT customer_id) as unique_keys,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_keys
FROM tbl_dim_customer
UNION ALL
SELECT 
  'tbl_fact_shipments' as table_name,
  COUNT(*) as row_count,
  COUNT(DISTINCT shipment_id) as unique_keys,
  SUM(CASE WHEN shipment_id IS NULL THEN 1 ELSE 0 END) as null_keys
FROM tbl_fact_shipments;
```

### Performance Monitoring

Real-time performance monitoring and optimization:

```sql
-- Performance monitoring view
CREATE VIEW vw_performance_monitoring AS
WITH query_history AS (
  SELECT
    query_id,
    query_text,
    user_name,
    warehouse_name,
    database_name,
    schema_name,
    query_type,
    start_time,
    end_time,
    total_elapsed_time / 1000 AS total_elapsed_time_seconds,
    bytes_scanned,
    rows_produced,
    compilation_time / 1000 AS compilation_time_seconds,
    execution_time / 1000 AS execution_time_seconds,
    queued_provisioning_time / 1000 AS queued_provisioning_time_seconds,
    queued_repair_time / 1000 AS queued_repair_time_seconds,
    queued_overload_time / 1000 AS queued_overload_time_seconds,
    error_code,
    error_message
  FROM snowflake.account_usage.query_history
  WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
),
warehouse_metering AS (
  SELECT
    start_time,
    end_time,
    warehouse_id,
    warehouse_name,
    credits_used_compute,
    credits_used_cloud_services
  FROM snowflake.account_usage.warehouse_metering_history
  WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
),
table_storage AS (
  SELECT
    table_id,
    table_name,
    table_schema,
    table_catalog,
    active_bytes / (1024*1024*1024) AS active_gb,
    time_travel_bytes / (1024*1024*1024) AS time_travel_gb,
    failsafe_bytes / (1024*1024*1024) AS failsafe_gb,
    row_count,
    last_altered
  FROM snowflake.account_usage.tables
  WHERE deleted IS NULL
)
SELECT
  'query_performance' AS metric_category,
  qh.query_id AS metric_id,
  qh.query_text AS metric_description,
  qh.start_time AS metric_timestamp,
  qh.total_elapsed_time_seconds AS metric_value,
  'seconds' AS metric_unit,
  qh.user_name,
  qh.warehouse_name,
  qh.database_name,
  qh.schema_name,
  qh.query_type,
  qh.bytes_scanned,
  qh.rows_produced,
  qh.error_message
FROM query_history qh
WHERE qh.query_type IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE')
UNION ALL
SELECT
  'warehouse_usage' AS metric_category,
  wm.warehouse_name AS metric_id,
  'Warehouse compute and cloud services credits used' AS metric_description,
  wm.start_time AS metric_timestamp,
  wm.credits_used_compute + wm.credits_used_cloud_services AS metric_value,
  'credits' AS metric_unit,
  NULL AS user_name,
  wm.warehouse_name,
  NULL AS database_name,
  NULL AS schema_name,
  NULL AS query_type,
  NULL AS bytes_scanned,
  NULL AS rows_produced,
  NULL AS error_message
FROM warehouse_metering wm
UNION ALL
SELECT
  'table_storage' AS metric_category,
  ts.table_name AS metric_id,
  'Table storage in GB' AS metric_description,
  ts.last_altered AS metric_timestamp,
  ts.active_gb AS metric_value,
  'GB' AS metric_unit,
  NULL AS user_name,
  NULL AS warehouse_name,
  ts.table_catalog AS database_name,
  ts.table_schema,
  NULL AS query_type,
  NULL AS bytes_scanned,
  ts.row_count AS rows_produced,
  NULL AS error_message
FROM table_storage ts;
```

### Cost Monitoring

Real-time cost monitoring and budget alerts:

```sql
-- Cost monitoring view
CREATE VIEW vw_cost_monitoring AS
WITH daily_warehouse_costs AS (
  SELECT
    DATE_TRUNC('day', start_time) AS cost_date,
    warehouse_name,
    SUM(credits_used_compute + credits_used_cloud_services) AS daily_credits_used,
    SUM(credits_used_compute + credits_used_cloud_services) * 3.00 AS estimated_daily_cost_usd
  FROM snowflake.account_usage.warehouse_metering_history
  WHERE start_time >= DATEADD(month, -3, CURRENT_TIMESTAMP())
  GROUP BY 1, 2
),
monthly_cost_summary AS (
  SELECT
    DATE_TRUNC('month', cost_date) AS month,
    warehouse_name,
    SUM(estimated_daily_cost_usd) AS monthly_cost_usd
  FROM daily_warehouse_costs
  GROUP BY 1, 2
),
cost_thresholds AS (
  SELECT 'COMPUTE_WH_XS' AS warehouse_name, 500 AS daily_cost_threshold_usd, 10000 AS monthly_cost_threshold_usd
  UNION ALL
  SELECT 'COMPUTE_WH_SMALL' AS warehouse_name, 1000 AS daily_cost_threshold_usd, 20000 AS monthly_cost_threshold_usd
  UNION ALL
  SELECT 'COMPUTE_WH_MEDIUM' AS warehouse_name, 2000 AS daily_cost_threshold_usd, 40000 AS monthly_cost_threshold_usd
)
SELECT
  dwc.cost_date,
  dwc.warehouse_name,
  dwc.daily_credits_used,
  dwc.estimated_daily_cost_usd,
  ct.daily_cost_threshold_usd,
  ct.monthly_cost_threshold_usd,
  CASE 
    WHEN dwc.estimated_daily_cost_usd > ct.daily_cost_threshold_usd THEN 'OVER_BUDGET'
    WHEN dwc.estimated_daily_cost_usd > ct.daily_cost_threshold_usd * 0.8 THEN 'APPROACHING_LIMIT'
    ELSE 'WITHIN_BUDGET'
  END AS cost_status,
  CASE 
    WHEN dwc.estimated_daily_cost_usd > ct.daily_cost_threshold_usd THEN 'CRITICAL'
    WHEN dwc.estimated_daily_cost_usd > ct.daily_cost_threshold_usd * 0.8 THEN 'WARNING'
    ELSE 'INFO'
  END AS alert_severity
FROM daily_warehouse_costs dwc
LEFT JOIN cost_thresholds ct ON dwc.warehouse_name = ct.warehouse_name
ORDER BY dwc.cost_date DESC, dwc.estimated_daily_cost_usd DESC;
```

## Alert System

### Email-based Alerting

Comprehensive email alerting system without Slack dependency:

```sql
-- Email alerting system
CREATE NOTIFICATION INTEGRATION email_notification_integration
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ('data_team@example.com', 'operations_team@example.com', 'finance_team@example.com')
  COMMENT = 'Email notification integration for system alerts';

-- Alert configuration table
CREATE TABLE alert_config (
  alert_name VARCHAR(100) PRIMARY KEY,
  alert_type VARCHAR(50),
  severity VARCHAR(20),
  threshold_value VARIANT,
  threshold_unit VARCHAR(50),
  recipients ARRAY,
  alert_message_template TEXT,
  enabled BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Alert history table
CREATE TABLE alert_history (
  alert_history_id VARCHAR(50) PRIMARY KEY DEFAULT UUID_STRING(),
  alert_name VARCHAR(100),
  alert_type VARCHAR(50),
  severity VARCHAR(20),
  alert_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  alert_message TEXT,
  alert_data VARIANT,
  recipients ARRAY,
  is_resolved BOOLEAN DEFAULT FALSE,
  resolved_by VARCHAR(255),
  resolved_at TIMESTAMP_NTZ
);
```

### Alert Tasks

Automated alerting tasks for different scenarios:

```sql
-- Data freshness monitoring task
CREATE TASK task_monitor_data_freshness
  WAREHOUSE = COMPUTE_WH_XS
  SCHEDULE = 'USING CRON 0 */2 * * * UTC'
  COMMENT = 'Monitors data freshness and triggers alerts if stale'
AS
BEGIN
  DECLARE
    freshness_alerts CURSOR FOR
      SELECT
        table_name,
        actual_value AS minutes_stale
      FROM vw_data_quality_sla
      WHERE sla_type = 'data_freshness' AND sla_result = 'FAIL';
    alert_data VARIANT;
  BEGIN
    FOR alert_row IN freshness_alerts DO
      alert_data := OBJECT_CONSTRUCT(
        'table_name', alert_row.table_name,
        'actual_value', alert_row.minutes_stale,
        'threshold_value', (SELECT threshold_value:minutes_since_sync FROM alert_config WHERE alert_name = 'data_freshness_critical')
      );
      CALL send_email_alert('data_freshness_critical', :alert_data);
    END FOR;
  END;
END;

-- Cost monitoring task
CREATE TASK task_monitor_cost_overruns
  WAREHOUSE = COMPUTE_WH_XS
  SCHEDULE = 'USING CRON 0 8 * * * UTC'
  COMMENT = 'Monitors warehouse costs and triggers alerts if thresholds are exceeded'
AS
BEGIN
  DECLARE
    cost_alerts CURSOR FOR
      SELECT
        warehouse_name,
        estimated_daily_cost_usd,
        daily_cost_threshold_usd,
        cost_status,
        alert_severity
      FROM vw_cost_monitoring
      WHERE cost_status != 'WITHIN_BUDGET';
    alert_data VARIANT;
  BEGIN
    FOR alert_row IN cost_alerts DO
      alert_data := OBJECT_CONSTRUCT(
        'warehouse_name', alert_row.warehouse_name,
        'actual_value', alert_row.estimated_daily_cost_usd,
        'threshold_value', alert_row.daily_cost_threshold_usd,
        'cost_status', alert_row.cost_status,
        'alert_severity', alert_row.alert_severity
      );
      CALL send_email_alert('cost_exceeded_daily', :alert_data);
    END FOR;
  END;
END;
```

### Notification Channels

Multiple notification channels for different alert types:

```sql
-- Alert configuration examples
INSERT INTO alert_config (alert_name, alert_type, severity, threshold_value, threshold_unit, recipients, alert_message_template) VALUES
('data_freshness_critical', 'data_freshness', 'CRITICAL', PARSE_JSON('{"minutes_since_sync": 360}'), 'minutes', ARRAY_CONSTRUCT('data_team@example.com', 'operations_team@example.com'), 'CRITICAL: Data for table {table_name} is {actual_value} minutes stale. SLA breached!'),
('cost_exceeded_daily', 'cost_monitoring', 'HIGH', PARSE_JSON('{"daily_cost_threshold_usd": 1000}'), 'usd', ARRAY_CONSTRUCT('finance_team@example.com', 'data_team@example.com'), 'HIGH: Daily warehouse cost for {warehouse_name} exceeded budget. Actual: ${actual_value} vs Threshold: ${threshold_value}'),
('data_quality_sla_fail', 'data_quality', 'CRITICAL', PARSE_JSON('{"sla_result": "FAIL"}'), 'status', ARRAY_CONSTRUCT('data_team@example.com'), 'CRITICAL: Data Quality SLA failed for {table_name} ({sla_type}). Status: {sla_status}'),
('dbt_run_failure', 'dbt_run_status', 'CRITICAL', PARSE_JSON('{"status": "ERROR"}'), 'status', ARRAY_CONSTRUCT('data_team@example.com'), 'CRITICAL: dbt run failed for model {model_name}. Error: {error_message}');
```

## Data Quality & Testing

### Comprehensive Testing Framework

16+ dbt tests covering business rules, data quality, and referential integrity:

```sql
-- Business rule tests
-- test_customer_tier_validation.sql
SELECT 
  customer_id,
  customer_tier,
  total_lifetime_value,
  CASE 
    WHEN total_lifetime_value > 100000 THEN 'PREMIUM'
    WHEN total_lifetime_value > 50000 THEN 'GOLD'
    WHEN total_lifetime_value > 10000 THEN 'SILVER'
    ELSE 'BRONZE'
  END as expected_tier
FROM tbl_dim_customer
WHERE customer_tier != expected_tier;

-- test_delivery_time_reasonableness.sql
SELECT 
  shipment_id,
  delivery_time_hours,
  distance_km
FROM tbl_fact_shipments
WHERE delivery_time_hours < 0 
   OR delivery_time_hours > 168  -- More than 1 week
   OR (distance_km > 0 AND delivery_time_hours = 0);

-- test_fuel_efficiency_reasonable.sql
SELECT 
  vehicle_id,
  fuel_efficiency_mpg,
  vehicle_type
FROM tbl_dim_vehicle
WHERE fuel_efficiency_mpg < 5 OR fuel_efficiency_mpg > 50;

-- test_maintenance_intervals.sql
SELECT 
  vehicle_id,
  last_maintenance_date,
  next_maintenance_due_date,
  maintenance_interval_miles
FROM tbl_dim_vehicle
WHERE next_maintenance_due_date < last_maintenance_date
   OR maintenance_interval_miles <= 0;

-- test_route_distance_positive.sql
SELECT 
  route_id,
  distance_km
FROM tbl_dim_route
WHERE distance_km <= 0;

-- test_shipment_status_logic.sql
SELECT 
  shipment_id,
  shipment_status,
  pickup_date,
  delivery_date,
  actual_delivery_date
FROM tbl_fact_shipments
WHERE (shipment_status = 'DELIVERED' AND actual_delivery_date IS NULL)
   OR (shipment_status = 'IN_TRANSIT' AND delivery_date < pickup_date)
   OR (shipment_status = 'PENDING' AND actual_delivery_date IS NOT NULL);
```

### Test Categories

#### Business Rule Validation
- Customer tier validation based on lifetime value
- Delivery time reasonableness checks
- Fuel efficiency range validation
- Maintenance interval logic
- Route distance validation
- Shipment status logic validation

#### Data Quality Checks
- Foreign key constraint validation
- ML feature store quality checks
- Analytics view consistency validation
- Null value detection
- Data type validation
- Range validation

#### Referential Integrity
- Customer dimension relationships
- Fact dimension relationships
- Vehicle dimension relationships
- Route dimension relationships
- Cross-table consistency checks

### Data Quality SLA

Automated data quality SLA monitoring:

```sql
-- Data quality SLA view
CREATE VIEW vw_data_quality_sla AS
WITH data_freshness_sla AS (
  SELECT
    'data_freshness' AS sla_type,
    table_name,
    CASE
      WHEN minutes_since_sync <= 60 THEN 'EXCELLENT'
      WHEN minutes_since_sync <= 360 THEN 'GOOD'
      WHEN minutes_since_sync <= 720 THEN 'ACCEPTABLE'
      ELSE 'POOR'
    END AS sla_status,
    CASE
      WHEN minutes_since_sync <= 360 THEN 'PASS'
      ELSE 'FAIL'
    END AS sla_result,
    360 AS sla_threshold_minutes,
    minutes_since_sync AS actual_value,
    'minutes' AS unit,
    CURRENT_TIMESTAMP() AS evaluation_timestamp
  FROM vw_fivetran_sync_status
),
data_completeness_sla AS (
  SELECT
    'completeness' AS sla_type,
    table_name,
    CASE
      WHEN null_keys = 0 THEN 'EXCELLENT'
      WHEN null_keys <= (row_count * 0.01) THEN 'GOOD'
      WHEN null_keys <= (row_count * 0.05) THEN 'ACCEPTABLE'
      ELSE 'POOR'
    END AS sla_status,
    CASE
      WHEN null_keys <= (row_count * 0.01) THEN 'PASS'
      ELSE 'FAIL'
    END AS sla_result,
    1 AS sla_threshold_percentage,
    (null_keys * 100.0 / NULLIF(row_count, 0)) AS actual_value,
    'percentage_null' AS unit,
    CURRENT_TIMESTAMP() AS evaluation_timestamp
  FROM vw_data_quality_summary
),
data_accuracy_sla AS (
  SELECT
    'accuracy' AS sla_type,
    'tbl_fact_shipments' AS table_name,
    CASE
      WHEN COUNT(CASE WHEN ABS(revenue - (delivery_cost + fuel_cost + driver_cost_usd)) > 10 THEN 1 END) = 0 THEN 'EXCELLENT'
      WHEN COUNT(CASE WHEN ABS(revenue - (delivery_cost + fuel_cost + driver_cost_usd)) > 10 THEN 1 END) <= (COUNT(*) * 0.01) THEN 'GOOD'
      ELSE 'POOR'
    END AS sla_status,
    CASE
      WHEN COUNT(CASE WHEN ABS(revenue - (delivery_cost + fuel_cost + driver_cost_usd)) > 10 THEN 1 END) <= (COUNT(*) * 0.01) THEN 'PASS'
      ELSE 'FAIL'
    END AS sla_result,
    1 AS sla_threshold_percentage,
    (COUNT(CASE WHEN ABS(revenue - (delivery_cost + fuel_cost + driver_cost_usd)) > 10 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0)) AS actual_value,
    'percentage_inaccurate' AS unit,
    CURRENT_TIMESTAMP() AS evaluation_timestamp
  FROM tbl_fact_shipments
)
SELECT * FROM data_freshness_sla
UNION ALL
SELECT * FROM data_completeness_sla
UNION ALL
SELECT * FROM data_accuracy_sla;
```

## ML Monitoring

### Feature Drift Detection

Monitor ML feature drift and model performance:

```sql
-- ML feature monitoring view
CREATE VIEW vw_ml_feature_monitoring AS
WITH feature_statistics AS (
  SELECT
    feature_name,
    feature_date,
    COUNT(*) as row_count,
    AVG(feature_value) as mean_value,
    STDDEV(feature_value) as stddev_value,
    MIN(feature_value) as min_value,
    MAX(feature_value) as max_value,
    COUNT(CASE WHEN feature_value IS NULL THEN 1 END) as null_count
  FROM tbl_ml_consolidated_feature_store
  WHERE feature_date >= DATEADD(day, -30, CURRENT_DATE())
  GROUP BY feature_name, feature_date
),
feature_drift AS (
  SELECT
    fs.feature_name,
    fs.feature_date,
    fs.mean_value,
    fs.stddev_value,
    LAG(fs.mean_value, 1) OVER (PARTITION BY fs.feature_name ORDER BY fs.feature_date) as prev_mean,
    LAG(fs.stddev_value, 1) OVER (PARTITION BY fs.feature_name ORDER BY fs.feature_date) as prev_stddev,
    CASE 
      WHEN ABS(fs.mean_value - prev_mean) > (prev_stddev * 2) THEN 'DRIFT_DETECTED'
      WHEN ABS(fs.stddev_value - prev_stddev) > (prev_stddev * 0.5) THEN 'VARIANCE_DRIFT'
      ELSE 'NORMAL'
    END as drift_status
  FROM feature_statistics fs
)
SELECT
  feature_name,
  feature_date,
  mean_value,
  stddev_value,
  prev_mean,
  prev_stddev,
  drift_status,
  ABS(mean_value - prev_mean) as mean_drift,
  ABS(stddev_value - prev_stddev) as stddev_drift
FROM feature_drift
WHERE drift_status != 'NORMAL'
ORDER BY feature_date DESC, ABS(mean_value - prev_mean) DESC;
```

### Model Performance Tracking

Track ML model performance and accuracy:

```sql
-- Model performance tracking
CREATE TABLE tbl_ml_model_performance (
  model_id VARCHAR(50),
  evaluation_date DATE,
  accuracy DECIMAL(5,4),
  precision_score DECIMAL(5,4),
  recall_score DECIMAL(5,4),
  f1_score DECIMAL(5,4),
  data_drift_score DECIMAL(5,4),
  model_drift_score DECIMAL(5,4),
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Model performance monitoring view
CREATE VIEW vw_model_performance_monitoring AS
SELECT
  model_id,
  evaluation_date,
  accuracy,
  precision_score,
  recall_score,
  f1_score,
  data_drift_score,
  model_drift_score,
  LAG(accuracy, 1) OVER (PARTITION BY model_id ORDER BY evaluation_date) as prev_accuracy,
  LAG(f1_score, 1) OVER (PARTITION BY model_id ORDER BY evaluation_date) as prev_f1_score,
  CASE 
    WHEN accuracy < (prev_accuracy * 0.95) THEN 'PERFORMANCE_DEGRADATION'
    WHEN data_drift_score > 0.2 THEN 'DATA_DRIFT_DETECTED'
    WHEN model_drift_score > 0.15 THEN 'MODEL_DRIFT_DETECTED'
    ELSE 'NORMAL'
  END as performance_status
FROM tbl_ml_model_performance
WHERE evaluation_date >= DATEADD(day, -30, CURRENT_DATE())
ORDER BY model_id, evaluation_date DESC;
```

## Monitoring Best Practices

### Key Metrics to Track

1. **Data Quality Metrics**
   - Null value percentages
   - Data freshness (time since last update)
   - Referential integrity violations
   - Business rule violations

2. **Performance Metrics**
   - Query execution times
   - Warehouse credit usage
   - Data processing throughput
   - Error rates

3. **Cost Metrics**
   - Daily/monthly warehouse costs
   - Credit consumption by warehouse
   - Cost per query/operation
   - Budget utilization

4. **ML Metrics**
   - Feature drift scores
   - Model accuracy trends
   - Prediction latency
   - Feature store freshness

### Alerting Thresholds

- **Data Freshness**: Alert if data is >6 hours stale
- **Data Quality**: Alert if >1% of records fail quality checks
- **Cost Overruns**: Alert if daily costs exceed budget by >20%
- **Performance**: Alert if query times exceed 5 minutes
- **ML Drift**: Alert if feature drift score >0.2

### Monitoring Dashboard

Create comprehensive monitoring dashboards:

```sql
-- Executive monitoring dashboard
CREATE VIEW vw_executive_monitoring AS
SELECT
  'data_quality' as metric_category,
  COUNT(CASE WHEN sla_result = 'FAIL' THEN 1 END) as failed_slas,
  COUNT(*) as total_slas,
  ROUND(COUNT(CASE WHEN sla_result = 'PASS' THEN 1 END) * 100.0 / COUNT(*), 2) as sla_compliance_pct
FROM vw_data_quality_sla
UNION ALL
SELECT
  'cost_management' as metric_category,
  COUNT(CASE WHEN cost_status = 'OVER_BUDGET' THEN 1 END) as over_budget_days,
  COUNT(*) as total_days,
  ROUND(SUM(estimated_daily_cost_usd), 2) as total_cost_usd
FROM vw_cost_monitoring
WHERE cost_date >= DATEADD(day, -30, CURRENT_DATE())
UNION ALL
SELECT
  'performance' as metric_category,
  COUNT(CASE WHEN total_elapsed_time_seconds > 300 THEN 1 END) as slow_queries,
  COUNT(*) as total_queries,
  ROUND(AVG(total_elapsed_time_seconds), 2) as avg_query_time_seconds
FROM vw_performance_monitoring
WHERE metric_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP());
```
