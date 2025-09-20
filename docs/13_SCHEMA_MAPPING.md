# Schema Mapping - Logistics Analytics Platform

This document provides comprehensive mapping of all objects to their respective schemas across different environments and layers.

## Environment Schema Structure

### Production Environment (`LOGISTICS_DW_PROD`)

| Schema | Purpose | Objects | Access Level |
|--------|---------|---------|--------------|
| **RAW** | Raw data ingestion from external sources | 7 raw tables | DATA_ENGINEER |
| **STAGING** | Data cleaning and standardization | 9 staging tables | DATA_ENGINEER |
| **MARTS** | Business logic and star schema | 18 marts objects | DATA_ANALYST |
| **ML_FEATURES** | ML feature engineering | 5 ML feature tables | ML_ENGINEER |
| **ANALYTICS** | Business intelligence views | 7 analytics views | DATA_ANALYST |
| **MONITORING** | System monitoring and alerting | 5 monitoring views | DATA_ENGINEER |
| **SNAPSHOTS** | Change data capture | 4 snapshot tables | DATA_ENGINEER |
| **ML_OBJECTS** | ML model registry and serving | 3 ML objects | ML_ENGINEER |
| **GOVERNANCE** | Data governance and lineage | 1 governance table | DATA_STEWARD |
| **PERFORMANCE** | Performance optimization | 6 performance views | DATA_ENGINEER |
| **SECURITY** | Security and access control | 4 security objects | SECURITY_ADMIN |

### Development Environment (`LOGISTICS_DW_DEV`)

| Schema | Purpose | Objects | Access Level |
|--------|---------|---------|--------------|
| **RAW** | Raw data ingestion (dev) | 7 raw tables | DATA_ENGINEER |
| **STAGING** | Data cleaning (dev) | 9 staging tables | DATA_ENGINEER |
| **MARTS** | Business logic (dev) | 18 marts objects | DATA_ANALYST |
| **ML_FEATURES** | ML features (dev) | 5 ML feature tables | ML_ENGINEER |
| **ANALYTICS** | Analytics views (dev) | 7 analytics views | DATA_ANALYST |
| **MONITORING** | Monitoring (dev) | 5 monitoring views | DATA_ENGINEER |

### Staging Environment (`LOGISTICS_DW_STAGING`)

| Schema | Purpose | Objects | Access Level |
|--------|---------|---------|--------------|
| **RAW** | Raw data ingestion (staging) | 7 raw tables | DATA_ENGINEER |
| **STAGING** | Data cleaning (staging) | 9 staging tables | DATA_ENGINEER |
| **MARTS** | Business logic (staging) | 18 marts objects | DATA_ANALYST |
| **ML_FEATURES** | ML features (staging) | 5 ML feature tables | ML_ENGINEER |
| **ANALYTICS** | Analytics views (staging) | 7 analytics views | DATA_ANALYST |
| **MONITORING** | Monitoring (staging) | 5 monitoring views | DATA_ENGINEER |

## Object-to-Schema Mapping

### Raw Layer (`RAW` Schema)

| Object | Full Path | Source | Purpose |
|--------|-----------|--------|---------|
| `tbl_raw_azure_customers` | `LOGISTICS_DW_PROD.RAW.tbl_raw_azure_customers` | Azure SQL Database | Customer master data |
| `tbl_raw_azure_shipments` | `LOGISTICS_DW_PROD.RAW.tbl_raw_azure_shipments` | Azure SQL Database | Shipment transactions |
| `tbl_raw_azure_vehicles` | `LOGISTICS_DW_PROD.RAW.tbl_raw_azure_vehicles` | Azure SQL Database | Vehicle master data |
| `tbl_raw_azure_maintenance` | `LOGISTICS_DW_PROD.RAW.tbl_raw_azure_maintenance` | Azure SQL Database | Maintenance records |
| `tbl_raw_weather_data` | `LOGISTICS_DW_PROD.RAW.tbl_raw_weather_data` | Weather API | Weather conditions |
| `tbl_raw_traffic_data` | `LOGISTICS_DW_PROD.RAW.tbl_raw_traffic_data` | Traffic API | Traffic conditions |
| `tbl_raw_telematics_data` | `LOGISTICS_DW_PROD.RAW.tbl_raw_telematics_data` | Telematics API | Vehicle telemetry |

### Staging Layer (`STAGING` Schema)

| Object | Full Path | Source | Purpose |
|--------|-----------|--------|---------|
| `tbl_stg_customers` | `LOGISTICS_DW_PROD.STAGING.tbl_stg_customers` | `RAW.tbl_raw_azure_customers` | Cleaned customer data |
| `tbl_stg_shipments` | `LOGISTICS_DW_PROD.STAGING.tbl_stg_shipments` | `RAW.tbl_raw_azure_shipments` | Cleaned shipment data |
| `tbl_stg_vehicles` | `LOGISTICS_DW_PROD.STAGING.tbl_stg_vehicles` | `RAW.tbl_raw_azure_vehicles` | Cleaned vehicle data |
| `tbl_stg_maintenance_logs` | `LOGISTICS_DW_PROD.STAGING.tbl_stg_maintenance_logs` | `RAW.tbl_raw_azure_maintenance` | Cleaned maintenance data |
| `tbl_stg_routes` | `LOGISTICS_DW_PROD.STAGING.tbl_stg_routes` | `RAW.tbl_raw_azure_shipments` | Route information |
| `tbl_stg_weather_conditions` | `LOGISTICS_DW_PROD.STAGING.tbl_stg_weather_conditions` | `RAW.tbl_raw_weather_data` | Cleaned weather data |
| `tbl_stg_traffic_conditions` | `LOGISTICS_DW_PROD.STAGING.tbl_stg_traffic_conditions` | `RAW.tbl_raw_traffic_data` | Cleaned traffic data |
| `tbl_stg_vehicle_telemetry` | `LOGISTICS_DW_PROD.STAGING.tbl_stg_vehicle_telemetry` | `RAW.tbl_raw_telematics_data` | Cleaned telemetry data |

### Marts Layer (`MARTS` Schema)

#### Dimension Tables

| Object | Full Path | Source | Purpose |
|--------|-----------|--------|---------|
| `tbl_dim_customer` | `LOGISTICS_DW_PROD.MARTS.tbl_dim_customer` | `STAGING.tbl_stg_customers` | Customer dimension |
| `tbl_dim_date` | `LOGISTICS_DW_PROD.MARTS.tbl_dim_date` | Generated | Date dimension |
| `tbl_dim_location` | `LOGISTICS_DW_PROD.MARTS.tbl_dim_location` | `STAGING.tbl_stg_shipments` | Location dimension |
| `tbl_dim_route` | `LOGISTICS_DW_PROD.MARTS.tbl_dim_route` | `STAGING.tbl_stg_routes` | Route dimension |
| `tbl_dim_traffic_conditions` | `LOGISTICS_DW_PROD.MARTS.tbl_dim_traffic_conditions` | `STAGING.tbl_stg_traffic_conditions` | Traffic dimension |
| `tbl_dim_vehicle` | `LOGISTICS_DW_PROD.MARTS.tbl_dim_vehicle` | `STAGING.tbl_stg_vehicles` | Vehicle dimension |
| `tbl_dim_vehicle_maintenance` | `LOGISTICS_DW_PROD.MARTS.tbl_dim_vehicle_maintenance` | `STAGING.tbl_stg_maintenance_logs` | Maintenance dimension |
| `tbl_dim_weather` | `LOGISTICS_DW_PROD.MARTS.tbl_dim_weather` | `STAGING.tbl_stg_weather_conditions` | Weather dimension |

#### Fact Tables

| Object | Full Path | Source | Purpose |
|--------|-----------|--------|---------|
| `tbl_fact_route_conditions` | `LOGISTICS_DW_PROD.MARTS.tbl_fact_route_conditions` | Multiple staging tables | Route conditions fact |
| `tbl_fact_route_performance` | `LOGISTICS_DW_PROD.MARTS.tbl_fact_route_performance` | Multiple staging tables | Route performance fact |
| `tbl_fact_shipments` | `LOGISTICS_DW_PROD.MARTS.tbl_fact_shipments` | Multiple staging tables | Shipments fact |
| `tbl_fact_vehicle_telemetry` | `LOGISTICS_DW_PROD.MARTS.tbl_fact_vehicle_telemetry` | `STAGING.tbl_stg_vehicle_telemetry` | Vehicle telemetry fact |
| `tbl_fact_vehicle_utilization` | `LOGISTICS_DW_PROD.MARTS.tbl_fact_vehicle_utilization` | Multiple staging tables | Vehicle utilization fact |

### ML Features Layer (`ML_FEATURES` Schema)

| Object | Full Path | Source | Purpose |
|--------|-----------|--------|---------|
| `tbl_ml_consolidated_feature_store` | `LOGISTICS_DW_PROD.ML_FEATURES.tbl_ml_consolidated_feature_store` | Multiple marts tables | Consolidated ML features |
| `tbl_ml_customer_behavior_segments` | `LOGISTICS_DW_PROD.ML_FEATURES.tbl_ml_customer_behavior_segments` | `MARTS.tbl_fact_shipments` | Customer behavior features |
| `tbl_ml_haul_segmentation` | `LOGISTICS_DW_PROD.ML_FEATURES.tbl_ml_haul_segmentation` | `MARTS.tbl_fact_shipments` | Haul segmentation features |
| `tbl_ml_maintenance_features` | `LOGISTICS_DW_PROD.ML_FEATURES.tbl_ml_maintenance_features` | `MARTS.tbl_fact_vehicle_telemetry` | Maintenance prediction features |
| `tbl_ml_rolling_analytics` | `LOGISTICS_DW_PROD.ML_FEATURES.tbl_ml_rolling_analytics` | Multiple marts tables | Rolling window features |

### Analytics Layer (`ANALYTICS` Schema)

| Object | Full Path | Source | Purpose |
|--------|-----------|--------|---------|
| `vw_ai_recommendations` | `LOGISTICS_DW_PROD.ANALYTICS.vw_ai_recommendations` | `ML_FEATURES.tbl_ml_consolidated_feature_store` | AI-driven recommendations |
| `vw_consolidated_dashboard` | `LOGISTICS_DW_PROD.ANALYTICS.vw_consolidated_dashboard` | Multiple marts tables | Executive dashboard |
| `vw_data_freshness_monitoring` | `LOGISTICS_DW_PROD.ANALYTICS.vw_data_freshness_monitoring` | System metadata | Data freshness monitoring |
| `vw_data_lineage` | `LOGISTICS_DW_PROD.ANALYTICS.vw_data_lineage` | System metadata | Data lineage tracking |
| `vw_kpi_dashboard` | `LOGISTICS_DW_PROD.ANALYTICS.vw_kpi_dashboard` | Multiple marts tables | KPI dashboard |
| `vw_operational_metrics` | `LOGISTICS_DW_PROD.ANALYTICS.vw_operational_metrics` | Multiple marts tables | Operational metrics |
| `vw_performance_dashboard` | `LOGISTICS_DW_PROD.ANALYTICS.vw_performance_dashboard` | Multiple marts tables | Performance dashboard |

### Monitoring Layer (`MONITORING` Schema)

| Object | Full Path | Source | Purpose |
|--------|-----------|--------|---------|
| `vw_cost_monitoring` | `LOGISTICS_DW_PROD.MONITORING.vw_cost_monitoring` | System metadata | Cost monitoring |
| `vw_data_quality_summary` | `LOGISTICS_DW_PROD.MONITORING.vw_data_quality_summary` | System metadata | Data quality summary |
| `vw_dbt_run_results` | `LOGISTICS_DW_PROD.MONITORING.vw_dbt_run_results` | dbt metadata | dbt run results |
| `vw_fivetran_sync_status` | `LOGISTICS_DW_PROD.MONITORING.vw_fivetran_sync_status` | Fivetran metadata | Fivetran sync status |
| `vw_performance_monitoring` | `LOGISTICS_DW_PROD.MONITORING.vw_performance_monitoring` | System metadata | Performance monitoring |

### ML Objects Layer (`ML_OBJECTS` Schema)

| Object | Full Path | Source | Purpose |
|--------|-----------|--------|---------|
| `tbl_ml_model_registry` | `LOGISTICS_DW_PROD.ML_OBJECTS.tbl_ml_model_registry` | ML training pipeline | ML model registry |
| `vw_ml_real_time_features` | `LOGISTICS_DW_PROD.ML_OBJECTS.vw_ml_real_time_features` | `ML_FEATURES.tbl_ml_consolidated_feature_store` | Real-time ML features |
| `vw_ml_real_time_maintenance` | `LOGISTICS_DW_PROD.ML_OBJECTS.vw_ml_real_time_maintenance` | `ML_FEATURES.tbl_ml_maintenance_features` | Real-time maintenance features |

### Governance Layer (`GOVERNANCE` Schema)

| Object | Full Path | Source | Purpose |
|--------|-----------|--------|---------|
| `tbl_data_asset_metadata` | `LOGISTICS_DW_PROD.GOVERNANCE.tbl_data_asset_metadata` | Manual/automated | Data asset metadata |
| `tbl_data_lineage` | `LOGISTICS_DW_PROD.GOVERNANCE.tbl_data_lineage` | Manual/automated | Data lineage relationships |
| `tbl_data_quality_scores` | `LOGISTICS_DW_PROD.GOVERNANCE.tbl_data_quality_scores` | Automated | Data quality scores |
| `tbl_business_impact` | `LOGISTICS_DW_PROD.GOVERNANCE.tbl_business_impact` | Manual/automated | Business impact assessment |

### Performance Layer (`PERFORMANCE` Schema)

| Object | Full Path | Source | Purpose |
|--------|-----------|--------|---------|
| `vw_predictive_warehouse_costs` | `LOGISTICS_DW_PROD.PERFORMANCE.vw_predictive_warehouse_costs` | System metadata | Predictive cost analysis |
| `vw_slow_and_inefficient_queries` | `LOGISTICS_DW_PROD.PERFORMANCE.vw_slow_and_inefficient_queries` | System metadata | Query performance analysis |
| `vw_query_optimization_recommendations` | `LOGISTICS_DW_PROD.PERFORMANCE.vw_query_optimization_recommendations` | System metadata | Query optimization recommendations |

### Security Layer (`SECURITY` Schema)

| Object | Full Path | Source | Purpose |
|--------|-----------|--------|---------|
| `tbl_audit_log` | `LOGISTICS_DW_PROD.SECURITY.tbl_audit_log` | System metadata | Audit logging |
| `tbl_data_classification` | `LOGISTICS_DW_PROD.SECURITY.tbl_data_classification` | Manual/automated | Data classification |
| `tbl_data_masking_policies` | `LOGISTICS_DW_PROD.SECURITY.tbl_data_masking_policies` | Manual/automated | Data masking policies |
| `tbl_row_level_security` | `LOGISTICS_DW_PROD.SECURITY.tbl_row_level_security` | Manual/automated | Row-level security policies |

### Snapshots Layer (`SNAPSHOTS` Schema)

| Object | Full Path | Source | Purpose |
|--------|-----------|--------|---------|
| `customers_snapshot` | `LOGISTICS_DW_PROD.SNAPSHOTS.customers_snapshot` | `RAW.tbl_raw_azure_customers` | Customer change tracking |
| `vehicles_snapshot` | `LOGISTICS_DW_PROD.SNAPSHOTS.vehicles_snapshot` | `RAW.tbl_raw_azure_vehicles` | Vehicle change tracking |
| `routes_snapshot` | `LOGISTICS_DW_PROD.SNAPSHOTS.routes_snapshot` | `RAW.tbl_raw_azure_shipments` | Route change tracking |
| `locations_snapshot` | `LOGISTICS_DW_PROD.SNAPSHOTS.locations_snapshot` | `RAW.tbl_raw_azure_shipments` | Location change tracking |

## Schema Dependencies

### Data Flow Dependencies

```
RAW Schema
    ↓
STAGING Schema
    ↓
MARTS Schema
    ↓
ML_FEATURES Schema
    ↓
ANALYTICS Schema
```

### Cross-Schema Dependencies

| Source Schema | Target Schema | Dependency Type | Objects |
|---------------|---------------|-----------------|---------|
| `RAW` | `STAGING` | Direct | All raw tables → staging tables |
| `STAGING` | `MARTS` | Direct | All staging tables → marts objects |
| `MARTS` | `ML_FEATURES` | Direct | Multiple marts tables → ML features |
| `ML_FEATURES` | `ANALYTICS` | Direct | ML features → analytics views |
| `MARTS` | `ANALYTICS` | Direct | Multiple marts tables → analytics views |
| `SYSTEM` | `MONITORING` | Metadata | System metadata → monitoring views |
| `SYSTEM` | `PERFORMANCE` | Metadata | System metadata → performance views |
| `SYSTEM` | `SECURITY` | Metadata | System metadata → security objects |

## Access Control Matrix

| Role | RAW | STAGING | MARTS | ML_FEATURES | ANALYTICS | MONITORING | SNAPSHOTS | ML_OBJECTS | GOVERNANCE | PERFORMANCE | SECURITY |
|------|-----|---------|-------|-------------|-----------|------------|-----------|------------|------------|-------------|----------|
| **DATA_ENGINEER** | R/W | R/W | R/W | R/W | R | R/W | R/W | R/W | R | R | R |
| **DATA_ANALYST** | R | R | R/W | R | R/W | R | R | R | R | R | R |
| **ML_ENGINEER** | R | R | R | R/W | R | R | R | R/W | R | R | R |
| **DATA_SCIENTIST** | R | R | R | R/W | R | R | R | R/W | R | R | R |
| **DATA_STEWARD** | R | R | R | R | R | R | R | R | R/W | R | R |
| **SECURITY_ADMIN** | R | R | R | R | R | R | R | R | R | R | R/W |
| **BUSINESS_USER** | - | - | R | - | R | - | - | - | - | - | - |

**Legend**: R = Read, W = Write, R/W = Read/Write, - = No Access

## Environment-Specific Schema Mapping

### Development Environment
- All schemas prefixed with `LOGISTICS_DW_DEV`
- Same structure as production
- Reduced access controls for testing

### Staging Environment
- All schemas prefixed with `LOGISTICS_DW_STAGING`
- Same structure as production
- Production-like access controls

### Production Environment
- All schemas prefixed with `LOGISTICS_DW_PROD`
- Full access controls
- Complete object set

## Schema Naming Conventions

### Table Naming
- **Raw Tables**: `tbl_raw_{source}_{entity}`
- **Staging Tables**: `tbl_stg_{entity}`
- **Dimension Tables**: `tbl_dim_{entity}`
- **Fact Tables**: `tbl_fact_{entity}`
- **ML Tables**: `tbl_ml_{purpose}`
- **Snapshot Tables**: `{entity}_snapshot`

### View Naming
- **Analytics Views**: `vw_{purpose}_dashboard`
- **Monitoring Views**: `vw_{purpose}_monitoring`
- **ML Views**: `vw_ml_{purpose}`
- **Performance Views**: `vw_{purpose}_performance`

### Schema Naming
- **Environment Prefix**: `LOGISTICS_DW_{ENV}`
- **Layer Suffix**: `{LAYER}` (RAW, STAGING, MARTS, etc.)
- **Purpose Suffix**: `{PURPOSE}` (ML_FEATURES, ANALYTICS, etc.)

## Maintenance and Updates

This schema mapping document should be updated whenever:
- New objects are added to any schema
- Schema structure changes
- Access controls are modified
- New environments are created
- Dependencies change

**Last Updated**: Generated automatically from project structure
**Next Review**: Quarterly or when schema changes occur
