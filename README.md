Smart Logistics Analytics Platform

Overview
This repository contains a production-style logistics analytics platform built on Snowflake and dbt, designed to demonstrate modern data engineering, advanced analytics, and MLOps. It includes end-to-end artifacts: Snowflake DDL, dbt models/macros/tests, operational monitoring, and a Python sample data generator.

Key capabilities
- Cost optimization and performance: clustering, task automation, warehouse sizing, query monitoring
- Quality and governance: comprehensive dbt tests, referential checks, data freshness
- Advanced analytics: 22 analytical views, rolling windows (7d/30d/90d), AI recommendations, sustainability metrics
- MLOps: feature store, real-time scoring, model monitoring, A/B testing hooks

Data architecture (5-layer)
1) RAW: landed data from source systems (Fivetran, COPY INTO)
2) STAGING: cleaned, typed, deduplicated views (stg_*)
3) MART: star-schema tables (dim_*, fact_*)
4) ANALYTICS: advanced/ML-ready views and feature sets
5) CONSUMPTION: BI tools, notebooks, APIs

Project structure
```text
├── LICENSE
├── README.md
├── data
│   └── generate_sample_data.py
├── dbt
│   ├── analyses
│   │   ├── cost_benefit_analysis.sql
│   │   ├── migration_impact_analysis.sql
│   │   └── performance_benchmarking.sql
│   ├── dbt_project.yml
│   ├── macros
│   │   ├── cost_calculations.sql
│   │   ├── data_quality_checks.sql
│   │   ├── date_helpers.sql
│   │   ├── logistics_calculations.sql
│   │   └── rolling_windows.sql
│   ├── models
│   │   ├── marts
│   │   │   ├── analytics
│   │   │   │   ├── ai_recommendations.sql
│   │   │   │   ├── data_freshness_monitoring.sql
│   │   │   │   ├── executive_dashboard_trending.sql
│   │   │   │   ├── performance_dashboard.sql
│   │   │   │   └── sustainability_metrics.sql
│   │   │   ├── dimensions
│   │   │   │   ├── dim_customer.sql
│   │   │   │   ├── dim_date.sql
│   │   │   │   ├── dim_location.sql
│   │   │   │   ├── dim_route.sql
│   │   │   │   ├── dim_traffic_conditions.sql
│   │   │   │   ├── dim_vehicle.sql
│   │   │   │   ├── dim_vehicle_maintenance.sql
│   │   │   │   └── dim_weather.sql
│   │   │   ├── facts
│   │   │   │   ├── fact_route_conditions.sql
│   │   │   │   ├── fact_route_performance.sql
│   │   │   │   ├── fact_shipments.sql
│   │   │   │   ├── fact_vehicle_telemetry.sql
│   │   │   │   └── fact_vehicle_utilization.sql
│   │   │   └── ml_features
│   │   │       ├── ml_customer_behavior_rolling.sql
│   │   │       ├── ml_customer_behavior_segments.sql
│   │   │       ├── ml_feature_store.sql
│   │   │       ├── ml_haul_segmentation.sql
│   │   │       ├── ml_maintenance_rolling_indicators.sql
│   │   │       ├── ml_operational_performance_rolling.sql
│   │   │       ├── ml_predictive_maintenance_features.sql
│   │   │       ├── ml_real_time_scoring.sql
│   │   │       ├── ml_route_optimization_features.sql
│   │   │       └── ml_route_performance_rolling.sql
│   │   ├── raw
│   │   │   └── _sources.yml
│   │   └── staging
│   │       ├── stg_customers.sql
│   │       ├── stg_shipments.sql
│   │       ├── stg_vehicles.sql
│   │       └── stg_routes.sql
│   ├── packages.yml
│   ├── profiles.yml
│   └── tests
│       ├── business_rules
│       ├── data_quality
│       │   └── test_vehicle_capacity_not_exceeded.sql
│       └── referential_integrity
├── fivetran
│   ├── connectors
│   └── monitoring
├── scripts
│   └── deployment
│       └── deploy_dbt_models.sh
├── snowflake
│   ├── optimization
│   ├── setup
│   ├── tables
│   │   ├── dimensions
│   │   └── facts
│   └── views
└── source-database
    ├── copy_into.sql
    ├── create_tables.sql
    └── insert_into.sql
```

Snowflake database objects
- 8 dimensions: date, customer, vehicle, location, route, weather, traffic, maintenance
- 5 facts: shipments, telemetry, route conditions, utilization, performance
- Clustering, constraints, warehouses, resource monitors (see snowflake/)

dbt project highlights
- Staging (views), marts (tables), analytics (views), ML features, monitoring
- Advanced macros for rolling windows and feature engineering
- Tests: data quality, business rules, referential integrity
- Incremental models and SCD2 snapshots (pattern-ready)

Sample data generator
- Location: data/generate_sample_data.py
- Output: logistics_sample_data/ with CSVs for dims/facts and a data-quality report
- Volumes (default): ~1k customers, 200 vehicles, 300+ locations, 150+ routes, 100k+ shipments, 50k telemetry, weather by city/day

Quick start
1) Generate sample data
   - pip install -r requirements.txt (or: pip install pandas numpy faker pyarrow)
   - python data/generate_sample_data.py

2) Create Snowflake schema (run in Snowflake)
   - See snowflake/setup/* and snowflake/tables/* for DDL

3) Load CSVs into Snowflake
   - Use source-database/copy_into.sql examples or your external stage

4) Configure dbt
   - Set env vars: SF_ACCOUNT, SF_USER, SF_PASSWORD, SF_ROLE, SF_DATABASE, SF_WAREHOUSE, SF_SCHEMA
   - dbt deps --project-dir dbt
   - dbt build --project-dir dbt --target dev

5) Explore analytics and ML features
   - Views under snowflake/views and dbt/models/marts/analytics

Business impact (example outcomes)
- Cost optimization: 15–20% fuel savings, 25% Snowflake cost reduction
- Customer experience: 25% improvement in delivery predictability
- Operational excellence: proactive maintenance, real-time decisioning
- Scalability: modern data stack supporting growth

Interview readiness
- Migration (SQL Server → Snowflake), modern stack (Snowflake + dbt + Fivetran)
- MLOps with feature store, advanced SQL/time-series analytics
- Cost management and performance tuning

Next steps
- Run generator → load data → deploy dbt → validate tests → demo dashboards/ML