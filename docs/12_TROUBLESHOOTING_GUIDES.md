# Troubleshooting Guides - Logistics Analytics Platform

This document provides comprehensive troubleshooting procedures for common issues encountered in the logistics analytics platform.

## Table of Contents
1. [Common Issues Overview](#common-issues-overview)
2. [Data Pipeline Issues](#data-pipeline-issues)
3. [Performance Issues](#performance-issues)
4. [Data Quality Issues](#data-quality-issues)
5. [ML Model Issues](#ml-model-issues)
6. [Infrastructure Issues](#infrastructure-issues)
7. [Security Issues](#security-issues)
8. [Cost Issues](#cost-issues)
9. [Integration Issues](#integration-issues)
10. [Emergency Procedures](#emergency-procedures)

## Common Issues Overview

### Issue Classification
- **Data Issues**: Missing data, incorrect data, data quality problems
- **Performance Issues**: Slow queries, high costs, resource constraints
- **Infrastructure Issues**: Warehouse problems, connectivity issues
- **ML Issues**: Model failures, prediction errors, drift detection
- **Security Issues**: Access problems, authentication failures
- **Integration Issues**: API failures, data source problems

### Escalation Matrix
| Issue Type | Level 1 | Level 2 | Level 3 | Level 4 |
|------------|---------|---------|---------|---------|
| Data Issues | Data Engineer | Senior Data Engineer | Data Engineering Lead | CTO |
| Performance Issues | Data Engineer | Senior Data Engineer | Data Engineering Lead | CTO |
| Infrastructure Issues | DevOps Engineer | Senior DevOps Engineer | Infrastructure Lead | CTO |
| ML Issues | ML Engineer | Senior ML Engineer | ML Engineering Lead | CTO |
| Security Issues | Security Engineer | Senior Security Engineer | Security Lead | CISO |
| Integration Issues | Integration Engineer | Senior Integration Engineer | Integration Lead | CTO |

## Data Pipeline Issues

### Issue: Data Not Appearing in Target Tables

#### Symptoms
- Tables show no new data
- Data freshness alerts triggered
- dbt runs complete but no data inserted

#### Diagnosis Steps
```sql
-- Check data freshness
SELECT 
    table_name,
    minutes_since_sync,
    last_sync_time
FROM vw_data_freshness_monitoring
WHERE table_name = 'tbl_fact_shipments';

-- Check source data
SELECT COUNT(*) as source_count
FROM {{ source('raw_logistics', 'shipments') }}
WHERE _loaded_at >= DATEADD('day', -1, CURRENT_TIMESTAMP());

-- Check target data
SELECT COUNT(*) as target_count
FROM tbl_fact_shipments
WHERE DATE(pickup_date) = CURRENT_DATE();
```

#### Common Causes
1. **Source System Issues**
   - Fivetran connector failure
   - Source system downtime
   - API rate limiting

2. **dbt Configuration Issues**
   - Incorrect incremental logic
   - Wrong unique key configuration
   - Schema changes not handled

3. **Data Quality Issues**
   - Data validation failures
   - Referential integrity violations
   - Business rule violations

#### Resolution Steps
1. **Check Source System**
```bash
# Check Fivetran connector status
SELECT * FROM vw_fivetran_sync_status
WHERE connector_name = 'shipments'
AND last_sync_time < DATEADD('hour', -2, CURRENT_TIMESTAMP());
```

2. **Verify dbt Configuration**
```yaml
# Check dbt_project.yml
models:
  logistics_analytics_platform:
    marts:
      facts:
        tbl_fact_shipments:
          materialized: incremental
          unique_key: shipment_id
          merge_update_columns: ['customer_id', 'vehicle_id', 'route_id']
```

3. **Check Data Quality**
```sql
-- Check for data quality issues
SELECT * FROM vw_data_quality_summary
WHERE table_name = 'tbl_fact_shipments'
AND sla_result = 'FAIL';
```

4. **Manual Data Refresh**
```bash
# Force full refresh
dbt run --full-refresh --select tbl_fact_shipments

# Check for errors
dbt run --select tbl_fact_shipments --store-failures
```

#### Prevention
- Monitor source system health
- Implement data quality checks
- Use proper incremental configuration
- Regular testing of data pipelines

### Issue: Data Quality Failures

#### Symptoms
- Data quality tests failing
- SLA breaches
- Inconsistent data across tables

#### Diagnosis Steps
```sql
-- Check data quality summary
SELECT 
    table_name,
    sla_type,
    sla_status,
    sla_result,
    actual_value,
    threshold_value
FROM vw_data_quality_sla
WHERE sla_result = 'FAIL'
ORDER BY evaluation_timestamp DESC;

-- Check specific data quality issues
SELECT 
    table_name,
    row_count,
    null_keys,
    (null_keys * 100.0 / row_count) as null_percentage
FROM vw_data_quality_summary
WHERE null_keys > 0
ORDER BY null_percentage DESC;
```

#### Common Causes
1. **Data Source Issues**
   - Source system changes
   - Data format changes
   - Missing required fields

2. **Business Rule Changes**
   - Updated validation rules
   - New data requirements
   - Changed business logic

3. **Data Processing Issues**
   - Transformation errors
   - Calculation mistakes
   - Data type mismatches

#### Resolution Steps
1. **Identify Root Cause**
```sql
-- Check for specific data quality issues
SELECT 
    shipment_id,
    customer_id,
    vehicle_id,
    route_id,
    pickup_date,
    delivery_date
FROM tbl_fact_shipments
WHERE customer_id IS NULL
OR vehicle_id IS NULL
OR route_id IS NULL
OR pickup_date IS NULL
OR delivery_date IS NULL;
```

2. **Fix Data Issues**
```sql
-- Update missing data
UPDATE tbl_fact_shipments
SET customer_id = 'DEFAULT_CUSTOMER'
WHERE customer_id IS NULL;

-- Validate fixes
SELECT COUNT(*) as null_count
FROM tbl_fact_shipments
WHERE customer_id IS NULL;
```

3. **Update Business Rules**
```sql
-- Update validation rules
ALTER TABLE tbl_fact_shipments
ADD CONSTRAINT check_customer_id
CHECK (customer_id IS NOT NULL);
```

4. **Re-run Data Quality Tests**
```bash
# Re-run data quality tests
dbt test --select tag:data_quality

# Check test results
SELECT * FROM test_results
WHERE test_name LIKE '%data_quality%'
ORDER BY test_timestamp DESC;
```

#### Prevention
- Regular data quality monitoring
- Automated data validation
- Source system change management
- Business rule documentation

## Performance Issues

### Issue: Slow Query Performance

#### Symptoms
- Queries taking > 5 minutes
- High warehouse costs
- User complaints about slow dashboards

#### Diagnosis Steps
```sql
-- Check query performance
SELECT 
    query_id,
    query_text,
    total_elapsed_time_seconds,
    bytes_scanned,
    rows_produced,
    compilation_time_seconds,
    execution_time_seconds
FROM vw_performance_monitoring
WHERE total_elapsed_time_seconds > 300
ORDER BY total_elapsed_time_seconds DESC;

-- Check warehouse usage
SELECT 
    warehouse_name,
    AVG(credits_used_compute) as avg_compute_credits,
    AVG(credits_used_cloud_services) as avg_cloud_credits,
    COUNT(*) as query_count
FROM vw_performance_monitoring
WHERE start_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
GROUP BY 1;
```

#### Common Causes
1. **Query Optimization Issues**
   - Missing indexes
   - Inefficient joins
   - Large table scans
   - Complex aggregations

2. **Data Volume Issues**
   - Large table sizes
   - Unnecessary data processing
   - Inefficient filtering

3. **Resource Constraints**
   - Undersized warehouse
   - Resource contention
   - Concurrent query limits

#### Resolution Steps
1. **Analyze Query Performance**
```sql
-- Analyze slow queries
SELECT 
    query_id,
    query_text,
    total_elapsed_time_seconds,
    bytes_scanned,
    rows_produced
FROM vw_performance_monitoring
WHERE total_elapsed_time_seconds > 300
ORDER BY total_elapsed_time_seconds DESC;
```

2. **Optimize Queries**
```sql
-- Add clustering keys
ALTER TABLE tbl_fact_shipments 
CLUSTER BY (pickup_date, customer_id);

-- Optimize joins
SELECT 
    s.shipment_id,
    c.customer_name,
    v.vehicle_type,
    r.route_name
FROM tbl_fact_shipments s
JOIN tbl_dim_customer c ON s.customer_id = c.customer_id
JOIN tbl_dim_vehicle v ON s.vehicle_id = v.vehicle_id
JOIN tbl_dim_route r ON s.route_id = r.route_id
WHERE s.pickup_date >= DATEADD('day', -30, CURRENT_DATE());
```

3. **Optimize Warehouse Sizing**
```sql
-- Check warehouse usage
SELECT 
    warehouse_name,
    AVG(credits_used_compute) as avg_compute_credits,
    MAX(credits_used_compute) as max_compute_credits
FROM vw_performance_monitoring
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1;
```

4. **Implement Caching**
```sql
-- Create materialized views
CREATE MATERIALIZED VIEW mv_daily_shipments AS
SELECT 
    DATE(pickup_date) as shipment_date,
    COUNT(*) as shipment_count,
    AVG(profit_margin_pct) as avg_profit_margin
FROM tbl_fact_shipments
GROUP BY 1;
```

#### Prevention
- Regular query performance monitoring
- Proactive optimization
- Proper indexing strategy
- Resource planning

### Issue: High Warehouse Costs

#### Symptoms
- Daily costs exceeding budget
- Unexpected cost spikes
- High credit consumption

#### Diagnosis Steps
```sql
-- Check daily costs
SELECT 
    cost_date,
    warehouse_name,
    daily_cost_usd,
    daily_credits_used,
    cost_per_credit
FROM vw_cost_monitoring
WHERE cost_date >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY daily_cost_usd DESC;

-- Check cost trends
SELECT 
    DATE_TRUNC('day', start_time) as cost_date,
    SUM(credits_used_compute + credits_used_cloud_services) as total_credits,
    SUM(credits_used_compute + credits_used_cloud_services) * 3.00 as estimated_cost
FROM vw_performance_monitoring
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1;
```

#### Common Causes
1. **Inefficient Queries**
   - Large table scans
   - Complex aggregations
   - Unnecessary data processing

2. **Resource Over-provisioning**
   - Oversized warehouses
   - Unnecessary auto-scaling
   - Resource waste

3. **Data Volume Issues**
   - Large data volumes
   - Inefficient data processing
   - Unnecessary data retention

#### Resolution Steps
1. **Identify Cost Drivers**
```sql
-- Identify expensive queries
SELECT 
    query_id,
    query_text,
    credits_used_compute,
    credits_used_cloud_services,
    total_elapsed_time_seconds
FROM vw_performance_monitoring
WHERE start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY (credits_used_compute + credits_used_cloud_services) DESC;
```

2. **Optimize Resource Usage**
```sql
-- Check warehouse sizing
SELECT 
    warehouse_name,
    AVG(credits_used_compute) as avg_compute_credits,
    MAX(credits_used_compute) as max_compute_credits
FROM vw_performance_monitoring
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1;
```

3. **Implement Cost Controls**
```sql
-- Set resource monitors
CREATE RESOURCE MONITOR cost_monitor
WITH CREDIT_QUOTA = 1000
FREQUENCY = DAILY
START_TIMESTAMP = CURRENT_TIMESTAMP()
TRIGGERS
    ON 80 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND;
```

4. **Optimize Data Processing**
```sql
-- Implement incremental processing
{{ config(
    materialized='incremental',
    unique_key='shipment_id',
    merge_update_columns=['customer_id', 'vehicle_id', 'route_id']
) }}

SELECT 
    shipment_id,
    customer_id,
    vehicle_id,
    route_id,
    pickup_date,
    delivery_date
FROM {{ source('raw_logistics', 'shipments') }}
WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }});
```

#### Prevention
- Regular cost monitoring
- Resource optimization
- Cost controls and limits
- Efficient data processing

## Data Quality Issues

### Issue: Referential Integrity Violations

#### Symptoms
- Foreign key constraint violations
- Orphaned records
- Data inconsistency

#### Diagnosis Steps
```sql
-- Check referential integrity
SELECT 
    test_name,
    test_result,
    error_message,
    test_timestamp
FROM test_results
WHERE test_name LIKE '%referential%'
ORDER BY test_timestamp DESC;

-- Check for orphaned records
SELECT 
    'shipments' as table_name,
    COUNT(*) as orphaned_count
FROM tbl_fact_shipments s
LEFT JOIN tbl_dim_customer c ON s.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

#### Common Causes
1. **Data Source Issues**
   - Source system changes
   - Data synchronization problems
   - Missing reference data

2. **Data Processing Issues**
   - Transformation errors
   - Data loading problems
   - Schema changes

#### Resolution Steps
1. **Identify Orphaned Records**
```sql
-- Find orphaned shipments
SELECT 
    s.shipment_id,
    s.customer_id,
    s.vehicle_id,
    s.route_id
FROM tbl_fact_shipments s
LEFT JOIN tbl_dim_customer c ON s.customer_id = c.customer_id
LEFT JOIN tbl_dim_vehicle v ON s.vehicle_id = v.vehicle_id
LEFT JOIN tbl_dim_route r ON s.route_id = r.route_id
WHERE c.customer_id IS NULL
OR v.vehicle_id IS NULL
OR r.route_id IS NULL;
```

2. **Fix Orphaned Records**
```sql
-- Update orphaned records
UPDATE tbl_fact_shipments
SET customer_id = 'DEFAULT_CUSTOMER'
WHERE customer_id NOT IN (SELECT customer_id FROM tbl_dim_customer);
```

3. **Validate Fixes**
```sql
-- Re-run referential integrity tests
dbt test --select tag:referential_integrity
```

#### Prevention
- Regular referential integrity checks
- Source system validation
- Data quality monitoring
- Proper error handling

## ML Model Issues

### Issue: Model Prediction Failures

#### Symptoms
- Model predictions failing
- Low confidence scores
- Prediction errors

#### Diagnosis Steps
```sql
-- Check model performance
SELECT 
    model_name,
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    last_updated
FROM tbl_ml_model_registry
WHERE status = 'ACTIVE'
ORDER BY last_updated DESC;

-- Check prediction quality
SELECT 
    model_name,
    COUNT(*) as prediction_count,
    AVG(confidence_score) as avg_confidence,
    SUM(CASE WHEN confidence_score < 0.7 THEN 1 ELSE 0 END) as low_confidence_count
FROM vw_ml_real_time_features
WHERE prediction_timestamp >= DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY 1;
```

#### Common Causes
1. **Data Quality Issues**
   - Missing features
   - Data drift
   - Feature quality problems

2. **Model Issues**
   - Model degradation
   - Training data problems
   - Feature engineering issues

#### Resolution Steps
1. **Check Feature Quality**
```sql
-- Check feature drift
SELECT 
    feature_name,
    drift_score,
    threshold,
    CASE 
        WHEN drift_score > threshold THEN 'DRIFT_DETECTED'
        ELSE 'NORMAL'
    END as drift_status
FROM vw_ml_feature_monitoring
WHERE evaluation_timestamp >= DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY drift_score DESC;
```

2. **Retrain Model**
```bash
# Trigger model retraining
dbt run --select tag:ml_retraining

# Validate new model
dbt test --select tag:ml_validation
```

3. **Update Feature Store**
```sql
-- Update feature store
SELECT 
    feature_name,
    COUNT(*) as feature_count,
    MIN(feature_date) as earliest_date,
    MAX(feature_date) as latest_date
FROM tbl_ml_consolidated_feature_store
WHERE feature_date >= DATEADD('day', -1, CURRENT_DATE())
GROUP BY 1;
```

#### Prevention
- Regular model monitoring
- Feature drift detection
- Automated retraining
- Model validation

## Infrastructure Issues

### Issue: Warehouse Connectivity Problems

#### Symptoms
- Connection timeouts
- Query failures
- Authentication errors

#### Diagnosis Steps
```bash
# Test warehouse connectivity
snowsql -c logistics_analytics_platform -q "SELECT CURRENT_TIMESTAMP();"

# Check warehouse status
SHOW WAREHOUSES;
```

#### Common Causes
1. **Network Issues**
   - Connectivity problems
   - Firewall restrictions
   - DNS resolution issues

2. **Authentication Issues**
   - Expired credentials
   - Permission problems
   - Account issues

#### Resolution Steps
1. **Check Network Connectivity**
```bash
# Test network connectivity
ping snowflake.com
telnet snowflake.com 443
```

2. **Verify Authentication**
```bash
# Test authentication
snowsql -c logistics_analytics_platform -q "SELECT CURRENT_USER();"
```

3. **Check Permissions**
```sql
-- Check user permissions
SHOW GRANTS TO USER CURRENT_USER();
```

#### Prevention
- Regular connectivity testing
- Credential management
- Network monitoring
- Permission auditing

## Security Issues

### Issue: Access Control Problems

#### Symptoms
- Permission denied errors
- Authentication failures
- Unauthorized access attempts

#### Diagnosis Steps
```sql
-- Check user access
SELECT 
    user_name,
    role_name,
    last_login_time,
    failed_login_count
FROM vw_user_access_monitoring
WHERE last_login_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY last_login_time DESC;

-- Check data access
SELECT 
    user_name,
    table_name,
    access_type,
    access_count,
    last_access_time
FROM vw_data_access_monitoring
WHERE last_access_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY access_count DESC;
```

#### Common Causes
1. **Permission Issues**
   - Incorrect role assignments
   - Missing permissions
   - Expired access

2. **Authentication Issues**
   - Password problems
   - Account lockouts
   - Credential issues

#### Resolution Steps
1. **Check User Permissions**
```sql
-- Check user roles
SHOW GRANTS TO USER 'username';

-- Check role permissions
SHOW GRANTS TO ROLE 'role_name';
```

2. **Fix Permission Issues**
```sql
-- Grant necessary permissions
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE data_analyst;
GRANT SELECT ON TABLE tbl_fact_shipments TO ROLE data_analyst;
```

3. **Reset Authentication**
```sql
-- Reset user password
ALTER USER 'username' SET PASSWORD = 'new_password';
```

#### Prevention
- Regular permission audits
- Access monitoring
- Security policies
- User training

## Cost Issues

### Issue: Unexpected Cost Spikes

#### Symptoms
- Daily costs exceeding budget
- Unexpected credit consumption
- Cost alerts triggered

#### Diagnosis Steps
```sql
-- Check daily costs
SELECT 
    cost_date,
    warehouse_name,
    daily_cost_usd,
    daily_credits_used,
    cost_per_credit
FROM vw_cost_monitoring
WHERE cost_date >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY daily_cost_usd DESC;

-- Check cost trends
SELECT 
    DATE_TRUNC('day', start_time) as cost_date,
    SUM(credits_used_compute + credits_used_cloud_services) as total_credits,
    SUM(credits_used_compute + credits_used_cloud_services) * 3.00 as estimated_cost
FROM vw_performance_monitoring
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1;
```

#### Common Causes
1. **Resource Over-provisioning**
   - Oversized warehouses
   - Unnecessary auto-scaling
   - Resource waste

2. **Inefficient Queries**
   - Large table scans
   - Complex aggregations
   - Unnecessary data processing

#### Resolution Steps
1. **Identify Cost Drivers**
```sql
-- Identify expensive queries
SELECT 
    query_id,
    query_text,
    credits_used_compute,
    credits_used_cloud_services,
    total_elapsed_time_seconds
FROM vw_performance_monitoring
WHERE start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY (credits_used_compute + credits_used_cloud_services) DESC;
```

2. **Implement Cost Controls**
```sql
-- Set resource monitors
CREATE RESOURCE MONITOR cost_monitor
WITH CREDIT_QUOTA = 1000
FREQUENCY = DAILY
START_TIMESTAMP = CURRENT_TIMESTAMP()
TRIGGERS
    ON 80 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND;
```

3. **Optimize Resource Usage**
```sql
-- Check warehouse sizing
SELECT 
    warehouse_name,
    AVG(credits_used_compute) as avg_compute_credits,
    MAX(credits_used_compute) as max_compute_credits
FROM vw_performance_monitoring
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1;
```

#### Prevention
- Regular cost monitoring
- Resource optimization
- Cost controls and limits
- Efficient data processing

## Integration Issues

### Issue: API Integration Failures

#### Symptoms
- API calls failing
- Data not syncing
- Integration errors

#### Diagnosis Steps
```sql
-- Check API integration status
SELECT 
    api_name,
    last_successful_call,
    last_failed_call,
    error_count,
    success_rate
FROM vw_api_integration_status
WHERE last_successful_call < DATEADD('hour', -2, CURRENT_TIMESTAMP())
ORDER BY last_failed_call DESC;
```

#### Common Causes
1. **API Issues**
   - API rate limiting
   - API changes
   - Authentication problems

2. **Network Issues**
   - Connectivity problems
   - Timeout issues
   - DNS resolution problems

#### Resolution Steps
1. **Check API Status**
```bash
# Test API connectivity
curl -H "Authorization: Bearer $API_TOKEN" https://api.example.com/status
```

2. **Verify API Configuration**
```yaml
# Check API configuration
api_config:
  base_url: "https://api.example.com"
  timeout: 30
  retry_count: 3
  rate_limit: 1000
```

3. **Fix API Issues**
```bash
# Restart API integration
dbt run --select tag:api_integration
```

#### Prevention
- Regular API monitoring
- Error handling
- Retry mechanisms
- API change management

## Emergency Procedures

### Critical System Failure

#### Immediate Response (0-15 minutes)
1. **Assess Impact**
   - Check system status
   - Identify affected users
   - Estimate recovery time

2. **Communicate**
   - Notify stakeholders
   - Update status page
   - Escalate to management

3. **Begin Recovery**
   - Start recovery procedures
   - Document actions taken
   - Monitor progress

#### Recovery Steps (15-60 minutes)
1. **System Recovery**
```bash
# Check system status
dbt run --select tag:incremental --store-failures

# Verify data integrity
SELECT COUNT(*) FROM tbl_fact_shipments WHERE DATE(pickup_date) = CURRENT_DATE();
```

2. **Data Recovery**
```sql
-- Check data freshness
SELECT 
    table_name,
    minutes_since_sync,
    last_sync_time
FROM vw_data_freshness_monitoring
WHERE minutes_since_sync > 360;
```

3. **Service Restoration**
   - Restore services
   - Verify functionality
   - Monitor performance

#### Post-Recovery (1-4 hours)
1. **Validation**
   - Verify system functionality
   - Check data integrity
   - Monitor performance

2. **Documentation**
   - Document incident
   - Update procedures
   - Plan improvements

3. **Communication**
   - Update stakeholders
   - Conduct post-mortem
   - Implement improvements

### Data Loss Incident

#### Immediate Response
1. **Assess Data Loss**
   - Identify affected data
   - Estimate data loss
   - Check backup availability

2. **Begin Recovery**
   - Restore from backup
   - Verify data integrity
   - Monitor recovery progress

#### Recovery Steps
1. **Data Restoration**
```sql
-- Restore from backup
CREATE TABLE tbl_fact_shipments_backup AS
SELECT * FROM tbl_fact_shipments
WHERE pickup_date >= DATEADD('day', -7, CURRENT_DATE());
```

2. **Data Validation**
```sql
-- Validate restored data
SELECT 
    COUNT(*) as record_count,
    MIN(pickup_date) as earliest_date,
    MAX(pickup_date) as latest_date
FROM tbl_fact_shipments_backup;
```

3. **Service Restoration**
   - Restore services
   - Verify functionality
   - Monitor performance

#### Prevention
- Regular backups
- Data validation
- Recovery testing
- Monitoring

---

**Last Updated**: December 2024  
**Version**: 1.0  
**Owner**: Data Engineering Team  
**Review Cycle**: Quarterly
