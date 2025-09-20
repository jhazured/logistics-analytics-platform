# Operational Runbooks - Logistics Analytics Platform

This document provides step-by-step operational procedures for managing the logistics analytics platform in production environments.

## Table of Contents
1. [Daily Operations](#daily-operations)
2. [Weekly Operations](#weekly-operations)
3. [Monthly Operations](#monthly-operations)
4. [Incident Response](#incident-response)
5. [Data Pipeline Operations](#data-pipeline-operations)
6. [ML Model Operations](#ml-model-operations)
7. [Performance Monitoring](#performance-monitoring)
8. [Backup and Recovery](#backup-and-recovery)
9. [Security Operations](#security-operations)
10. [Cost Management](#cost-management)

## Daily Operations

### Morning Checklist (8:00 AM)
**Duration**: 30 minutes
**Responsible**: Data Engineering Team

#### 1. System Health Check
```bash
# Check dbt run status
dbt run --select tag:incremental --store-failures

# Verify data freshness
SELECT table_name, minutes_since_sync 
FROM vw_data_freshness_monitoring 
WHERE minutes_since_sync > 360;

# Check warehouse status
SHOW WAREHOUSES;
```

#### 2. Data Quality Validation
```sql
-- Check data quality scores
SELECT * FROM vw_data_quality_summary 
WHERE sla_result = 'FAIL';

-- Verify referential integrity
SELECT * FROM test_foreign_key_constraints 
WHERE test_result = 'FAIL';
```

#### 3. Performance Monitoring
```sql
-- Check query performance
SELECT * FROM vw_performance_monitoring 
WHERE total_elapsed_time_seconds > 300
ORDER BY start_time DESC;

-- Monitor warehouse costs
SELECT * FROM vw_cost_monitoring 
WHERE daily_cost_usd > 1000;
```

#### 4. Alert Review
- Review overnight alerts
- Check email notifications
- Verify critical system status
- Update incident log if needed

### Afternoon Checklist (4:00 PM)
**Duration**: 20 minutes
**Responsible**: Data Engineering Team

#### 1. End-of-Day Validation
```sql
-- Verify daily data completeness
SELECT COUNT(*) as shipment_count 
FROM tbl_fact_shipments 
WHERE DATE(pickup_date) = CURRENT_DATE();

-- Check ML feature freshness
SELECT MAX(feature_date) as latest_features 
FROM tbl_ml_consolidated_feature_store;
```

#### 2. Performance Summary
- Generate daily performance report
- Review cost metrics
- Check data quality scores
- Update operational dashboard

#### 3. Preparation for Next Day
- Review scheduled maintenance
- Check upcoming data loads
- Verify resource availability
- Update team on any issues

## Weekly Operations

### Monday: Weekly Planning (9:00 AM)
**Duration**: 1 hour
**Responsible**: Data Engineering Team Lead

#### 1. Performance Review
```sql
-- Weekly performance summary
SELECT 
    DATE_TRUNC('week', date_key) as week,
    AVG(on_time_rate_pct) as avg_on_time_rate,
    SUM(total_revenue_usd) as weekly_revenue,
    AVG(profit_margin_pct) as avg_profit_margin
FROM vw_consolidated_dashboard 
WHERE date_key >= DATEADD('week', -4, CURRENT_DATE())
GROUP BY 1
ORDER BY 1;
```

#### 2. Capacity Planning
- Review warehouse usage trends
- Plan for upcoming data volumes
- Schedule maintenance windows
- Update resource allocations

#### 3. Team Coordination
- Review weekly objectives
- Assign tasks and responsibilities
- Schedule team meetings
- Update project timelines

### Wednesday: Mid-Week Check (2:00 PM)
**Duration**: 30 minutes
**Responsible**: Data Engineering Team

#### 1. Data Quality Assessment
```sql
-- Check data quality trends
SELECT 
    table_name,
    AVG(null_keys * 100.0 / row_count) as avg_null_percentage
FROM vw_data_quality_summary 
WHERE evaluation_timestamp >= DATEADD('week', -1, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC;
```

#### 2. Performance Optimization
- Review slow-running queries
- Check warehouse utilization
- Optimize resource usage
- Update performance baselines

### Friday: Weekly Summary (5:00 PM)
**Duration**: 45 minutes
**Responsible**: Data Engineering Team Lead

#### 1. Weekly Report Generation
```sql
-- Generate weekly business metrics
SELECT 
    'Weekly Summary' as report_type,
    COUNT(DISTINCT customer_id) as active_customers,
    COUNT(*) as total_shipments,
    AVG(on_time_rate_pct) as avg_on_time_rate,
    SUM(total_revenue_usd) as total_revenue,
    AVG(profit_margin_pct) as avg_profit_margin
FROM vw_consolidated_dashboard 
WHERE date_key >= DATEADD('week', -1, CURRENT_DATE());
```

#### 2. Issue Resolution
- Review unresolved issues
- Update incident reports
- Plan remediation actions
- Communicate with stakeholders

#### 3. Next Week Preparation
- Review upcoming requirements
- Schedule maintenance activities
- Plan capacity adjustments
- Update operational procedures

## Monthly Operations

### First Monday: Monthly Planning (9:00 AM)
**Duration**: 2 hours
**Responsible**: Data Engineering Team Lead

#### 1. Monthly Performance Review
```sql
-- Monthly business metrics
SELECT 
    DATE_TRUNC('month', date_key) as month,
    COUNT(DISTINCT customer_id) as active_customers,
    COUNT(*) as total_shipments,
    AVG(on_time_rate_pct) as avg_on_time_rate,
    SUM(total_revenue_usd) as monthly_revenue,
    AVG(profit_margin_pct) as avg_profit_margin,
    SUM(total_carbon_emissions_kg) as total_emissions
FROM vw_consolidated_dashboard 
WHERE date_key >= DATEADD('month', -12, CURRENT_DATE())
GROUP BY 1
ORDER BY 1;
```

#### 2. Capacity and Cost Analysis
```sql
-- Monthly cost analysis
SELECT 
    DATE_TRUNC('month', cost_date) as month,
    SUM(estimated_daily_cost_usd) as monthly_cost,
    AVG(credits_used_compute) as avg_compute_credits,
    AVG(credits_used_cloud_services) as avg_cloud_credits
FROM vw_cost_monitoring 
WHERE cost_date >= DATEADD('month', -12, CURRENT_DATE())
GROUP BY 1
ORDER BY 1;
```

#### 3. Strategic Planning
- Review business objectives
- Plan infrastructure upgrades
- Schedule major maintenance
- Update operational procedures

### Mid-Month: Performance Optimization (15th, 2:00 PM)
**Duration**: 1 hour
**Responsible**: Data Engineering Team

#### 1. Query Performance Analysis
```sql
-- Identify slow queries
SELECT 
    query_id,
    query_text,
    total_elapsed_time_seconds,
    bytes_scanned,
    rows_produced
FROM vw_performance_monitoring 
WHERE total_elapsed_time_seconds > 60
AND start_time >= DATEADD('month', -1, CURRENT_TIMESTAMP())
ORDER BY total_elapsed_time_seconds DESC;
```

#### 2. Resource Optimization
- Review warehouse sizing
- Optimize clustering keys
- Update resource monitors
- Plan capacity adjustments

### Month-End: Comprehensive Review (Last Friday, 3:00 PM)
**Duration**: 2 hours
**Responsible**: Data Engineering Team Lead

#### 1. Monthly Business Report
```sql
-- Comprehensive monthly metrics
WITH monthly_metrics AS (
    SELECT 
        DATE_TRUNC('month', date_key) as month,
        COUNT(*) as total_shipments,
        AVG(on_time_rate_pct) as avg_on_time_rate,
        SUM(total_revenue_usd) as total_revenue,
        AVG(profit_margin_pct) as avg_profit_margin,
        SUM(total_carbon_emissions_kg) as total_emissions
    FROM vw_consolidated_dashboard 
    WHERE date_key >= DATEADD('month', -12, CURRENT_DATE())
    GROUP BY 1
)
SELECT 
    month,
    total_shipments,
    avg_on_time_rate,
    total_revenue,
    avg_profit_margin,
    total_emissions,
    LAG(total_shipments) OVER (ORDER BY month) as prev_month_shipments,
    LAG(total_revenue) OVER (ORDER BY month) as prev_month_revenue,
    (total_shipments - LAG(total_shipments) OVER (ORDER BY month)) * 100.0 / 
    LAG(total_shipments) OVER (ORDER BY month) as shipment_growth_pct,
    (total_revenue - LAG(total_revenue) OVER (ORDER BY month)) * 100.0 / 
    LAG(total_revenue) OVER (ORDER BY month) as revenue_growth_pct
FROM monthly_metrics
ORDER BY month;
```

#### 2. Data Quality Assessment
```sql
-- Monthly data quality trends
SELECT 
    table_name,
    AVG(CASE WHEN sla_result = 'PASS' THEN 1.0 ELSE 0.0 END) as pass_rate,
    COUNT(*) as total_checks,
    SUM(CASE WHEN sla_result = 'FAIL' THEN 1 ELSE 0 END) as failures
FROM vw_data_quality_sla 
WHERE evaluation_timestamp >= DATEADD('month', -1, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY pass_rate ASC;
```

#### 3. Strategic Review
- Review business objectives achievement
- Plan next month's priorities
- Update operational procedures
- Communicate with stakeholders

## Incident Response

### Incident Classification
- **P1 (Critical)**: System down, data loss, security breach
- **P2 (High)**: Performance degradation, data quality issues
- **P3 (Medium)**: Minor issues, non-critical failures
- **P4 (Low)**: Cosmetic issues, minor bugs

### P1 Incident Response (Critical)
**Response Time**: 15 minutes
**Resolution Time**: 4 hours

#### 1. Immediate Response
```bash
# Check system status
dbt run --select tag:incremental --store-failures

# Verify data integrity
SELECT COUNT(*) FROM tbl_fact_shipments WHERE DATE(pickup_date) = CURRENT_DATE();

# Check warehouse status
SHOW WAREHOUSES;
```

#### 2. Communication
- Notify stakeholders immediately
- Create incident ticket
- Update status page
- Escalate to management

#### 3. Resolution Steps
- Identify root cause
- Implement fix
- Verify resolution
- Update documentation

#### 4. Post-Incident
- Conduct post-mortem
- Update procedures
- Implement preventive measures
- Communicate lessons learned

### P2 Incident Response (High)
**Response Time**: 1 hour
**Resolution Time**: 24 hours

#### 1. Assessment
```sql
-- Check data quality
SELECT * FROM vw_data_quality_summary WHERE sla_result = 'FAIL';

-- Monitor performance
SELECT * FROM vw_performance_monitoring 
WHERE total_elapsed_time_seconds > 300;
```

#### 2. Resolution
- Implement temporary fix
- Monitor impact
- Plan permanent solution
- Update stakeholders

### P3/P4 Incident Response (Medium/Low)
**Response Time**: 4 hours
**Resolution Time**: 1 week

#### 1. Documentation
- Log incident details
- Assess impact
- Plan resolution
- Schedule fix

## Data Pipeline Operations

### Daily Pipeline Monitoring
**Schedule**: Every 4 hours
**Responsible**: Data Engineering Team

#### 1. Data Freshness Check
```sql
-- Check data freshness
SELECT 
    table_name,
    minutes_since_sync,
    CASE 
        WHEN minutes_since_sync <= 60 THEN 'EXCELLENT'
        WHEN minutes_since_sync <= 360 THEN 'GOOD'
        WHEN minutes_since_sync <= 720 THEN 'ACCEPTABLE'
        ELSE 'POOR'
    END as freshness_status
FROM vw_data_freshness_monitoring
ORDER BY minutes_since_sync DESC;
```

#### 2. Pipeline Performance
```sql
-- Check pipeline performance
SELECT 
    model_name,
    run_duration_seconds,
    status,
    error_message
FROM vw_dbt_run_results 
WHERE last_run_at >= DATEADD('hour', -4, CURRENT_TIMESTAMP())
ORDER BY run_duration_seconds DESC;
```

#### 3. Data Quality Validation
```sql
-- Validate data quality
SELECT 
    table_name,
    row_count,
    null_keys,
    (null_keys * 100.0 / row_count) as null_percentage
FROM vw_data_quality_summary
WHERE evaluation_timestamp >= DATEADD('hour', -4, CURRENT_TIMESTAMP())
ORDER BY null_percentage DESC;
```

### Pipeline Maintenance
**Schedule**: Weekly
**Responsible**: Data Engineering Team

#### 1. Performance Optimization
```sql
-- Identify slow models
SELECT 
    model_name,
    AVG(run_duration_seconds) as avg_duration,
    COUNT(*) as run_count
FROM vw_dbt_run_results 
WHERE last_run_at >= DATEADD('week', -1, CURRENT_TIMESTAMP())
GROUP BY 1
HAVING avg_duration > 300
ORDER BY avg_duration DESC;
```

#### 2. Resource Optimization
- Review warehouse usage
- Optimize clustering keys
- Update materialization strategies
- Plan capacity adjustments

## ML Model Operations

### Daily Model Monitoring
**Schedule**: Every 6 hours
**Responsible**: ML Engineering Team

#### 1. Model Performance Check
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
```

#### 2. Feature Drift Detection
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

#### 3. Prediction Monitoring
```sql
-- Monitor prediction quality
SELECT 
    model_name,
    COUNT(*) as prediction_count,
    AVG(confidence_score) as avg_confidence,
    SUM(CASE WHEN confidence_score < 0.7 THEN 1 ELSE 0 END) as low_confidence_count
FROM vw_ml_real_time_features
WHERE prediction_timestamp >= DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY 1;
```

### Weekly Model Maintenance
**Schedule**: Every Sunday
**Responsible**: ML Engineering Team

#### 1. Model Retraining
```bash
# Trigger model retraining
dbt run --select tag:ml_retraining

# Validate new model performance
dbt test --select tag:ml_validation
```

#### 2. Feature Store Update
```sql
-- Update feature store
SELECT 
    feature_name,
    COUNT(*) as feature_count,
    MIN(feature_date) as earliest_date,
    MAX(feature_date) as latest_date
FROM tbl_ml_consolidated_feature_store
WHERE feature_date >= DATEADD('week', -1, CURRENT_DATE())
GROUP BY 1;
```

## Performance Monitoring

### Real-time Monitoring
**Schedule**: Continuous
**Responsible**: Data Engineering Team

#### 1. System Performance
```sql
-- Monitor system performance
SELECT 
    warehouse_name,
    AVG(credits_used_compute) as avg_compute_credits,
    AVG(credits_used_cloud_services) as avg_cloud_credits,
    COUNT(*) as query_count
FROM vw_performance_monitoring
WHERE start_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
GROUP BY 1;
```

#### 2. Query Performance
```sql
-- Monitor query performance
SELECT 
    query_type,
    AVG(total_elapsed_time_seconds) as avg_duration,
    MAX(total_elapsed_time_seconds) as max_duration,
    COUNT(*) as query_count
FROM vw_performance_monitoring
WHERE start_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY avg_duration DESC;
```

### Performance Optimization
**Schedule**: Weekly
**Responsible**: Data Engineering Team

#### 1. Slow Query Analysis
```sql
-- Identify slow queries
SELECT 
    query_id,
    query_text,
    total_elapsed_time_seconds,
    bytes_scanned,
    rows_produced
FROM vw_performance_monitoring
WHERE total_elapsed_time_seconds > 60
AND start_time >= DATEADD('week', -1, CURRENT_TIMESTAMP())
ORDER BY total_elapsed_time_seconds DESC;
```

#### 2. Resource Optimization
- Review warehouse sizing
- Optimize clustering keys
- Update resource monitors
- Plan capacity adjustments

## Backup and Recovery

### Daily Backup Verification
**Schedule**: Daily at 2:00 AM
**Responsible**: Data Engineering Team

#### 1. Backup Status Check
```sql
-- Check backup status
SELECT 
    table_name,
    last_backup_time,
    backup_size_gb,
    backup_status
FROM vw_backup_status
WHERE last_backup_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY last_backup_time DESC;
```

#### 2. Recovery Testing
```bash
# Test recovery procedures
dbt run --select tag:recovery_test

# Verify data integrity
dbt test --select tag:integrity_check
```

### Weekly Recovery Testing
**Schedule**: Every Sunday at 3:00 AM
**Responsible**: Data Engineering Team

#### 1. Full Recovery Test
```bash
# Test full recovery
dbt run --select tag:full_recovery_test

# Validate data consistency
dbt test --select tag:consistency_check
```

#### 2. Recovery Documentation
- Update recovery procedures
- Test recovery scripts
- Validate backup integrity
- Update recovery documentation

## Security Operations

### Daily Security Monitoring
**Schedule**: Every 4 hours
**Responsible**: Security Team

#### 1. Access Monitoring
```sql
-- Monitor user access
SELECT 
    user_name,
    role_name,
    last_login_time,
    failed_login_count
FROM vw_user_access_monitoring
WHERE last_login_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY last_login_time DESC;
```

#### 2. Data Access Monitoring
```sql
-- Monitor data access
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

### Weekly Security Review
**Schedule**: Every Friday
**Responsible**: Security Team

#### 1. Security Assessment
```sql
-- Security assessment
SELECT 
    security_metric,
    current_value,
    threshold,
    status
FROM vw_security_monitoring
WHERE evaluation_date >= DATEADD('week', -1, CURRENT_DATE())
ORDER BY status DESC;
```

#### 2. Access Review
- Review user access rights
- Update security policies
- Monitor compliance
- Update security documentation

## Cost Management

### Daily Cost Monitoring
**Schedule**: Every 6 hours
**Responsible**: Data Engineering Team

#### 1. Cost Analysis
```sql
-- Daily cost analysis
SELECT 
    warehouse_name,
    daily_cost_usd,
    daily_credits_used,
    cost_per_credit,
    cost_trend
FROM vw_cost_monitoring
WHERE cost_date >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY daily_cost_usd DESC;
```

#### 2. Budget Monitoring
```sql
-- Budget monitoring
SELECT 
    budget_category,
    monthly_budget_usd,
    current_spend_usd,
    remaining_budget_usd,
    budget_utilization_pct
FROM vw_budget_monitoring
WHERE month = DATE_TRUNC('month', CURRENT_DATE())
ORDER BY budget_utilization_pct DESC;
```

### Weekly Cost Optimization
**Schedule**: Every Monday
**Responsible**: Data Engineering Team Lead

#### 1. Cost Optimization Analysis
```sql
-- Cost optimization opportunities
SELECT 
    optimization_type,
    potential_savings_usd,
    implementation_effort,
    priority_level
FROM vw_cost_optimization_opportunities
WHERE status = 'ACTIVE'
ORDER BY potential_savings_usd DESC;
```

#### 2. Resource Planning
- Review resource usage
- Plan capacity adjustments
- Optimize warehouse sizing
- Update cost budgets

---

**Last Updated**: December 2024  
**Version**: 1.0  
**Owner**: Data Engineering Team  
**Review Cycle**: Monthly
