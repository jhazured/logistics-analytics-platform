# Architecture Overview

## Hybrid ML-Optimized Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   ML SERVING    │    │   ML FEATURES   │    │      MART       │
│                 │    │                 │    │                 │
│ • Real-time     │    │ • Feature Store │    │ • Fact Tables   │
│   Inference     │◄───│ • Model Registry│◄───│ • Dimensions    │
│ • Low-latency   │    │ • ML Monitoring │    │ • Star Schema   │
│   Features      │    │ • Versioning    │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   STAGING       │
                    │                 │
                    │ • Data Cleaning │
                    │ • Standardization│
                    │ • Validation    │
                    └─────────────────┘
                                 │
                    ┌─────────────────┐
                    │     RAW         │
                    │                 │
                    │ • Incremental   │
                    │ • Change Capture│
                    │ • Cost Optimized│
                    └─────────────────┘
```

## ML Data Product Layers

| Layer | Technology | Purpose | Optimization |
|-------|------------|---------|--------------|
| **Raw Data** | Fivetran + Snowflake | Data ingestion | Incremental loading |
| **Staging** | dbt | Data cleaning | Automated validation |
| **Marts** | dbt | Business logic | Star schema design |
| **ML Features** | Snowflake Tables | Feature storage | Clustered for ML queries |
| **ML Serving** | Snowflake Views | Real-time inference | Materialized views |
| **Model Registry** | Snowflake Tables | Model lifecycle | Performance tracking |
| **Ingestion** | Fivetran | Automated data pipeline | Real-time data |
| **Orchestration** | GitHub Actions | CI/CD and workflow management | ML pipeline automation |
| **Monitoring** | Custom Python | Data quality and performance monitoring | Feature drift detection |
| **Security** | Snowflake RBAC | Role-based access control and data masking | ML data protection |

## Technology Stack

| Component | Technology | Purpose | Key Features |
|-----------|------------|---------|--------------|
| **Data Warehouse** | Snowflake | Centralized data storage | Multi-cluster, auto-scaling |
| **Data Transformation** | dbt Core 1.6+ | ELT pipeline | Version control, testing |
| **Data Integration** | Fivetran | Automated data ingestion | 300+ connectors |
| **ML/AI Platform** | Snowflake ML | Model training & serving | Native ML functions |
| **Feature Store** | Snowflake Tables | ML feature management | Versioning, lineage |
| **Model Registry** | Snowflake Tables | Model lifecycle | Performance tracking |
| **Real-time Processing** | Snowflake Streams | Change data capture | Low-latency updates |
| **Orchestration** | GitHub Actions | CI/CD automation | Automated testing |
| **Monitoring** | Custom Python | Data quality & performance | Real-time alerting |
| **Security** | Snowflake RBAC | Access control | Row-level security |

## Data Model

### Dimensional Design

The platform implements a **star schema** optimized for both analytical queries and ML feature engineering:

#### Dimension Tables (8 dimensions)
- **`tbl_dim_customer`** - Customer master data with segmentation
- **`tbl_dim_vehicle`** - Vehicle specifications and maintenance history
- **`tbl_dim_location`** - Geographic locations with coordinates
- **`tbl_dim_route`** - Route definitions with optimization metrics
- **`tbl_dim_date`** - Date dimension with business calendar
- **`tbl_dim_traffic_conditions`** - Traffic pattern analysis
- **`tbl_dim_vehicle_maintenance`** - Maintenance schedules and history
- **`tbl_dim_weather`** - Weather conditions and impact factors

#### Fact Tables (5 facts with incremental loading)
- **`tbl_fact_shipments`** - Core shipment transactions with calculated metrics
- **`tbl_fact_vehicle_telemetry`** - Real-time vehicle sensor data
- **`tbl_fact_vehicle_utilization`** - Vehicle usage and efficiency metrics
- **`tbl_fact_route_performance`** - Route optimization and performance data
- **`tbl_fact_route_conditions`** - Environmental and traffic conditions

### Machine Learning Data Product (5 ML models)

#### ML Feature Engineering (dbt Models)
- **`tbl_ml_consolidated_feature_store`** - Centralized feature repository
- **`tbl_ml_rolling_analytics`** - Time-series features with rolling windows
- **`tbl_ml_maintenance_features`** - Predictive maintenance features
- **`tbl_ml_customer_behavior_segments`** - Customer segmentation features
- **`tbl_ml_haul_segmentation`** - Route and shipment classification

#### ML-Optimized Infrastructure (Snowflake)
- **Feature Store Tables** - Clustered for ML query performance
- **Model Registry** - Complete model lifecycle management
- **Real-time Serving Views** - Low-latency feature serving
- **ML Monitoring** - Feature drift and model performance tracking

## Cost Optimization Strategy

### Incremental Loading Implementation

This platform implements a comprehensive incremental loading strategy to minimize Fivetran costs and optimize data processing performance:

#### **Cost Savings**
- **70-90% reduction** in Fivetran data processing charges
- **50-70% reduction** in Snowflake compute costs
- **60-80% faster** incremental build times

#### **Implementation Details**
- **Raw Models**: All 7 raw models use `materialized='incremental'` with merge strategies
- **Unique Keys**: Proper primary key configuration for efficient merging
- **Incremental Logic**: Based on `_loaded_at` timestamps for change detection
- **Data Retention**: Optimized windows (7-30 days) for external API data

#### **Operational Commands**
```bash
# Initial setup (full refresh)
dbt run --full-refresh --select tag:raw

# Cost-optimized incremental updates
dbt run --select tag:incremental

# Monitor incremental performance
dbt run --select tag:incremental --store-failures
```

> **📋 Detailed Strategy**: See [07_INCREMENTAL_LOADING_STRATEGY.md](07_INCREMENTAL_LOADING_STRATEGY.md) for complete implementation guide, monitoring procedures, and cost analysis.

## Project Structure

```
logistics-analytics-platform/
├── 📄 LICENSE                                    # MIT License
├── 📄 README.md                                  # Project overview
├── 📄 requirements.txt                           # Python dependencies
├── 📁 docs/                                      # Documentation
│   ├── 00_README.md                              # This comprehensive documentation
│   ├── 01_ARCHITECTURE.md                        # Architecture and design
│   ├── 02_SETUP.md                               # Setup and deployment
│   ├── 03_ML_GUIDE.md                            # ML/AI engineer guide
│   ├── 04_ADVANCED_FEATURES.md                   # Advanced features
│   ├── 05_MONITORING.md                          # Monitoring and testing
│   ├── 06_BUSINESS_IMPACT.md                     # Business value and ROI
│   ├── 07_INCREMENTAL_LOADING_STRATEGY.md        # Cost optimization guide
│   └── 08_INDEX.md                               # File index with GitHub URLs
├── 📁 .github/workflows/                         # CI/CD pipelines
│   ├── dbt_ci_cd.yml                            # Main dbt CI/CD pipeline
│   ├── dbt-docs.yml                             # Documentation generation
│   └── dbt.yml                                  # dbt workflow configuration
├── 📁 data/                                      # Sample data generation
│   └── generate_sample_data.py                  # Python script for test data
├── 📁 fivetran/                                  # Fivetran monitoring and management
│   └── 📁 monitoring/                           # Fivetran connector monitoring (3 files)
│       ├── connector_health_check.sql
│       ├── data_quality_alerts.sql
│       └── sync_monitoring.sql
├── 📁 dbt/                                       # dbt project root
│   ├── 📄 dbt_project.yml                       # Enhanced multi-environment configuration
│   ├── 📄 packages.yml                          # Package dependencies (dbt_utils, dbt_expectations)
│   ├── 📄 profiles.yml                          # Multi-environment Snowflake profiles
│   ├── 📄 exposures.yml                         # dbt exposures for downstream tools
│   ├── 📁 macros/                               # Enhanced reusable macros (8 files)
│   │   ├── aggregations.sql                     # Aggregation and rolling window macros
│   │   ├── business_logic.sql                   # Business logic and calculations
│   │   ├── cost_calculations.sql                # Business cost calculations
│   │   ├── data_types.sql                       # Data type conversions and casting
│   │   ├── date_time.sql                        # Date and time utility macros
│   │   ├── error_handling.sql                   # Error handling and validation
│   │   ├── post_hooks.sql                       # Post-hook macros for optimization
│   │   └── rolling_windows.sql                  # Rolling window calculations
│   ├── 📁 models/                               # dbt models organized by layer
│   │   ├── 📁 marts/                            # Business logic layer
│   │   │   ├── 📁 analytics/                    # Analytics and reporting (7 models)
│   │   │   │   ├── vw_ai_recommendations.sql
│   │   │   │   ├── vw_consolidated_dashboard.sql
│   │   │   │   ├── vw_data_freshness_monitoring.sql
│   │   │   │   ├── vw_data_lineage.sql
│   │   │   │   ├── vw_data_quality_sla.sql
│   │   │   │   ├── schema.yml
│   │   │   │   └── vw_sustainability_metrics.sql
│   │   │   ├── 📁 dimensions/                   # Dimension tables (8 models)
│   │   │   │   ├── tbl_dim_customer.sql
│   │   │   │   ├── tbl_dim_date.sql
│   │   │   │   ├── tbl_dim_location.sql
│   │   │   │   ├── tbl_dim_route.sql
│   │   │   │   ├── tbl_dim_traffic_conditions.sql
│   │   │   │   ├── tbl_dim_vehicle.sql
│   │   │   │   ├── tbl_dim_vehicle_maintenance.sql
│   │   │   │   ├── tbl_dim_weather.sql
│   │   │   │   └── schema.yml
│   │   │   ├── 📁 facts/                        # Fact tables (5 models)
│   │   │   │   ├── tbl_fact_route_conditions.sql
│   │   │   │   ├── tbl_fact_route_performance.sql
│   │   │   │   ├── tbl_fact_shipments.sql
│   │   │   │   ├── tbl_fact_vehicle_telemetry.sql
│   │   │   │   ├── tbl_fact_vehicle_utilization.sql
│   │   │   │   └── schema.yml
│   │   │   └── 📁 ml_features/                  # ML feature engineering (5 models)
│   │   │       ├── tbl_ml_consolidated_feature_store.sql
│   │   │       ├── tbl_ml_rolling_analytics.sql
│   │   │       ├── tbl_ml_maintenance_features.sql
│   │   │       ├── tbl_ml_customer_behavior_segments.sql
│   │   │       └── tbl_ml_haul_segmentation.sql
│   │   └── 📁 ml_serving/                       # Real-time ML serving models
│   │       ├── vw_ml_real_time_customer_features.sql
│   │       └── vw_ml_real_time_vehicle_features.sql
│   │   ├── 📁 raw/                              # Incremental source definitions (8 models)
│   │   │   ├── _sources.yml
│   │   │   ├── tbl_raw_azure_customers.sql       # Incremental with merge strategy
│   │   │   ├── tbl_raw_azure_shipments.sql       # Incremental with merge strategy
│   │   │   ├── tbl_raw_azure_vehicles.sql        # Incremental with merge strategy
│   │   │   ├── tbl_raw_azure_maintenance.sql     # Incremental with merge strategy
│   │   │   ├── tbl_raw_weather_data.sql          # Incremental (30-day window)
│   │   │   ├── tbl_raw_traffic_data.sql          # Incremental (30-day window)
│   │   │   └── tbl_raw_telematics_data.sql       # Incremental (7-day window)
│   │   └── 📁 staging/                          # Data cleaning layer (9 models)
│   │       ├── tbl_stg_customers.sql
│   │       ├── tbl_stg_maintenance_logs.sql
│   │       ├── tbl_stg_routes.sql
│   │       ├── tbl_stg_shipments.sql
│   │       ├── tbl_stg_traffic_conditions.sql
│   │       ├── tbl_stg_vehicle_telemetry.sql
│   │       ├── tbl_stg_vehicles.sql
│   │       ├── tbl_stg_weather_conditions.sql
│   │       └── schema.yml
│   ├── 📁 snapshots/                            # Change data capture (4 models)
│   │   ├── customers_snapshot.sql
│   │   ├── vehicles_snapshot.sql
│   │   ├── routes_snapshot.sql
│   │   └── locations_snapshot.sql
│   ├── 📁 tests/                                # Data quality tests (20+ tests)
│   │   ├── 📁 business_rules/                   # Business logic validation (8 tests)
│   │   │   ├── test_analytics_view_consistency.sql
│   │   │   ├── test_customer_tier_validation.sql
│   │   │   ├── test_kpi_calculations.sql
│   │   │   ├── test_maintenance_intervals.sql
│   │   │   ├── test_maintenance_schedule_compliance.sql
│   │   │   ├── test_route_efficiency_bounds.sql
│   │   │   ├── test_seasonal_demand_patterns.sql
│   │   │   └── test_shipment_status_logic.sql
│   │   ├── 📁 data_quality/                     # Data quality checks (7 tests)
│   │   │   ├── test_cost_reasonableness.sql
│   │   │   ├── test_data_freshness_monitoring.sql
│   │   │   ├── test_delivery_time_realistic.sql
│   │   │   ├── test_fuel_efficiency_reasonable.sql
│   │   │   ├── test_ml_feature_store_quality.sql
│   │   │   ├── test_referential_integrity_shipments.sql
│   │   │   ├── test_route_distance_positive.sql
│   │   │   └── test_vehicle_capacity_not_exceeded.sql
│   │   └── 📁 referential_integrity/            # Relationship validation (2 tests)
│   │       ├── test_fact_dimension_relationships.sql
│   │       └── test_foreign_key_constraints.sql
├── 📁 snowflake/                                # Snowflake-specific objects
│   ├── 📁 optimization/                         # Performance optimization (5 files)
│   │   ├── automated_tasks.sql
│   │   ├── clustering_keys.sql
│   │   ├── cost_monitoring.sql
│   │   ├── emergency_procedures.sql
│   │   └── performance_tuning.sql
│   ├── 📁 security/                             # Security and governance (4 files)
│   │   ├── audit_logging.sql
│   │   ├── data_classification.sql
│   │   ├── data_masking_policies.sql
│   │   └── row_level_security.sql
│   ├── 📁 setup/                                # Environment setup (5 files)
│   │   ├── 01_database_setup.sql
│   │   ├── 02_schema_creation.sql
│   │   ├── 03_warehouse_configuration.sql
│   │   ├── 04_user_roles_permissions.sql
│   │   └── 05_resource_monitors.sql
│   ├── 📁 streaming/                            # Real-time processing (7 files)
│   │   ├── alert_system.sql
│   │   ├── create_streams.sql
│   │   ├── create_tasks.sql
│   │   ├── deploy_streams_and_tasks.sql
│   │   ├── email_alerting_system.sql
│   │   ├── real_time_kpis.sql
│   │   └── task_management.sql
│   ├── 📁 tables/                               # ML-optimized table definitions
│   │   ├── 📁 dimensions/                       # Dimension tables (8 models)
│   │   │   ├── tbl_dim_customer.sql
│   │   │   ├── tbl_dim_date.sql
│   │   │   ├── tbl_dim_location.sql
│   │   │   ├── tbl_dim_route.sql
│   │   │   ├── tbl_dim_traffic_conditions.sql
│   │   │   ├── tbl_dim_vehicle.sql
│   │   │   ├── tbl_dim_vehicle_maintenance.sql
│   │   │   └── tbl_dim_weather.sql
│   │   └── 📁 facts/                            # Fact tables (5 models)
│   │       ├── tbl_fact_route_conditions.sql
│   │       ├── tbl_fact_route_performance.sql
│   │       ├── tbl_fact_shipments.sql
│   │       ├── tbl_fact_vehicle_telemetry.sql
│   │       └── tbl_fact_vehicle_utilization.sql
│   ├── 📁 views/                                # Business intelligence views
│   │   ├── 📁 cost_optimization/                # Cost optimization views (4 models)
│   │   │   ├── vw_monthly_cost_forecast.sql
│   │   │   ├── vw_query_cost_analysis.sql
│   │   │   ├── vw_resource_monitor_usage.sql
│   │   │   └── vw_warehouse_cost_analysis.sql
│   │   └── 📁 monitoring/                       # Monitoring views (5 models)
│   │       ├── vw_data_quality_summary.sql
│   │       ├── vw_dbt_run_results.sql
│   │       ├── vw_fivetran_sync_status.sql
│   │       ├── vw_performance_monitoring.sql
│   │       └── vw_cost_monitoring.sql
│   └── 📁 ml_objects/                           # ML-specific infrastructure
│       ├── 📁 model_registry/                   # ML model lifecycle management
│       │   └── tbl_ml_model_registry.sql
│       ├── 📁 serving_views/                    # Real-time ML serving (2 models)
│       │   ├── vw_ml_real_time_features.sql
│       │   └── vw_ml_real_time_maintenance.sql
│       └── 📁 monitoring/                       # ML monitoring & observability
│           └── vw_ml_feature_monitoring.sql
└── 📁 scripts/                                  # Utility scripts
    ├── 📁 setup/                                # Environment setup scripts
    │   └── configure_environment.sh
    └── 📁 deployment/                           # Deployment scripts
        └── deploy_dbt_models.sh
```

## Key Technical Achievements

This project demonstrates proficiency in:

### 🏗️ **Modern Data Engineering**
- **Snowflake + dbt + Fivetran**: Complete modern data stack implementation
- **Incremental Loading**: Comprehensive strategy reducing Fivetran costs by 70-90% with merge-based incremental processing
- **Real-time Processing**: Stream processing with Snowflake Streams and Tasks
- **Performance Optimization**: Environment-specific configurations and warehouse sizing
- **Cost Optimization**: Advanced incremental loading with proper unique keys, merge strategies, and data retention windows

### 🧪 **Data Quality & Testing**
- **16+ dbt Tests**: Comprehensive testing framework with business rules, data quality, and referential integrity
- **Automated Monitoring**: Real-time alerting and data quality validation
- **CI/CD Integration**: Automated testing in deployment pipelines

### 🔒 **Security & Governance**
- **Row-Level Security**: Customer and vehicle data protection
- **Data Masking**: PII protection with dynamic masking policies
- **Audit Logging**: Comprehensive data access and modification tracking
- **Role-Based Access**: Granular permissions for different user types

### 🚀 **DevOps & Automation**
- **GitHub Actions**: Automated CI/CD with testing and deployment
- **Environment Management**: Multi-environment configuration (dev/staging/prod)
- **Infrastructure as Code**: Automated Snowflake object creation and management

### 📊 **Business Intelligence**
- **Star Schema Design**: Optimized dimensional model for analytics
- **Advanced Analytics**: Rolling windows, trend analysis, and predictive metrics
- **Real-time Dashboards**: Live operational metrics and KPI monitoring

### 🤖 **MLOps & Advanced Analytics**
- **Feature Engineering**: Automated feature creation with dbt
- **Model Registry**: Complete ML model lifecycle management
- **Real-time Serving**: Low-latency feature serving for ML inference
- **ML Monitoring**: Feature drift detection and model performance tracking
