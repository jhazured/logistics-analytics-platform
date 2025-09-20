# Incremental Loading Strategy for Logistics Analytics Platform

## Overview
This document outlines the incremental loading strategy implemented to minimize Fivetran costs and optimize data processing performance.

## Cost Optimization Benefits

### Fivetran Cost Reduction
- **Before**: Full refresh of all data on every run
- **After**: Only process new/changed records since last run
- **Estimated Savings**: 70-90% reduction in Fivetran charges

### Performance Benefits
- **Faster Build Times**: Only process incremental data
- **Reduced Warehouse Costs**: Less compute time required
- **Better Resource Utilization**: Parallel processing of incremental batches

## Implementation Strategy

### 1. Raw Layer (Incremental Tables)

All raw models now use `materialized='incremental'` with the following configuration:

```sql
{{ config(
    materialized='incremental',
    unique_key='[primary_key]',
    merge_update_columns=['all_columns_except_primary_key'],
    tags=['raw', 'incremental']
) }}
```

#### Raw Models Configured:
- `tbl_raw_azure_customers` - Unique key: `customer_id`
- `tbl_raw_azure_shipments` - Unique key: `shipment_id`
- `tbl_raw_azure_vehicles` - Unique key: `vehicle_id`
- `tbl_raw_azure_maintenance` - Unique key: `maintenance_id`
- `tbl_raw_weather_data` - Unique key: `weather_id`
- `tbl_raw_traffic_data` - Unique key: `traffic_id`
- `tbl_raw_telematics_data` - Unique key: `telemetry_id`

### 2. Incremental Logic

Each raw model uses the following incremental logic:

```sql
{% if is_incremental() %}
    -- Only process records that are new or updated since last run
    AND _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}
```

### 3. Data Retention Windows

#### Azure Source Data (Customers, Shipments, Vehicles, Maintenance)
- **Retention**: All historical data
- **Incremental Strategy**: Based on `_loaded_at` timestamp
- **Update Frequency**: Daily

#### External API Data (Weather, Traffic, Telematics)
- **Weather**: 30-day rolling window
- **Traffic**: 30-day rolling window  
- **Telematics**: 7-day rolling window
- **Incremental Strategy**: Based on `_loaded_at` timestamp
- **Update Frequency**: Hourly (telematics), Daily (weather/traffic)

### 4. Snapshot Strategy

#### Change Detection Snapshots
- `customers_snapshot` - Tracks customer dimension changes
- `vehicles_snapshot` - Tracks vehicle dimension changes

#### Snapshot Configuration
```sql
{{
  config(
    target_schema='snapshots',
    unique_key='[primary_key]',
    strategy='check',
    check_cols=['key_columns_to_monitor']
  )
}}
```

### 5. Staging Layer

Staging models automatically benefit from incremental loading since they reference the incremental raw models:

```sql
with src as (
  select * from {{ source('raw', 'tbl_raw_azure_customers') }}
)
-- Processing logic remains the same
```

### 6. Mart Layer

#### Dimension Tables
- **Materialization**: `table` (for performance)
- **Refresh Strategy**: Incremental based on source changes
- **Update Frequency**: Daily

#### Fact Tables
- **Materialization**: `table` (for performance)
- **Refresh Strategy**: Incremental based on source changes
- **Update Frequency**: Daily

#### ML Feature Tables
- **Materialization**: `table` (for ML training)
- **Refresh Strategy**: Incremental based on source changes
- **Update Frequency**: Daily

#### Analytics Views
- **Materialization**: `view` (for real-time calculations)
- **Refresh Strategy**: Real-time (always current)
- **Update Frequency**: On-demand

## Operational Procedures

### 1. Initial Load
```bash
# Full refresh for initial setup
dbt run --full-refresh --select tag:raw
```

### 2. Incremental Runs
```bash
# Standard incremental run
dbt run --select tag:incremental
```

### 3. Monitoring
```bash
# Check incremental model performance
dbt run --select tag:incremental --store-failures
```

### 4. Troubleshooting
```bash
# Force full refresh if needed
dbt run --full-refresh --select tbl_raw_azure_customers
```

## Cost Monitoring

### Fivetran Metrics to Monitor
- **Rows Processed**: Should decrease significantly with incremental loading
- **Data Volume**: Monitor for unexpected spikes
- **Sync Frequency**: Optimize based on business needs

### dbt Metrics to Monitor
- **Build Time**: Should decrease with incremental loading
- **Warehouse Credits**: Monitor compute usage
- **Model Performance**: Track query execution times

## Best Practices

### 1. Incremental Key Selection
- Use immutable primary keys
- Avoid using timestamps as unique keys
- Consider composite keys for complex scenarios

### 2. Merge Strategy
- Use `merge_update_columns` to specify which columns to update
- Avoid updating columns that shouldn't change
- Handle soft deletes appropriately

### 3. Data Quality
- Implement data quality checks on incremental data
- Monitor for data drift in incremental loads
- Validate incremental logic regularly

### 4. Performance Optimization
- Index on incremental key columns
- Consider partitioning for large tables
- Monitor query performance over time

## Expected Results

### Cost Savings
- **Fivetran**: 70-90% reduction in data processing costs
- **Snowflake**: 50-70% reduction in compute costs
- **dbt**: Faster build times, reduced resource usage

### Performance Improvements
- **Build Time**: 60-80% faster incremental runs
- **Data Freshness**: More frequent updates possible
- **Resource Utilization**: Better parallel processing

### Operational Benefits
- **Scalability**: Handle larger data volumes efficiently
- **Reliability**: More robust data processing pipeline
- **Flexibility**: Easier to adjust update frequencies

## Monitoring and Alerting

### Key Metrics to Track
1. **Incremental Load Success Rate**
2. **Data Volume per Run**
3. **Build Time per Model**
4. **Cost per Run**
5. **Data Freshness**

### Alerting Thresholds
- **Failed Incremental Loads**: Immediate alert
- **Unusual Data Volume**: >200% of normal
- **Extended Build Times**: >300% of normal
- **Cost Spikes**: >150% of normal

This incremental loading strategy provides a robust, cost-effective approach to data processing while maintaining data quality and performance.
