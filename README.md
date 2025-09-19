Project structure

├── LICENSE
├── README.md
├── data
│   └── generate_sample_data.py
├── dbt
│   ├── _models.yml
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
│   │   │   ├── _sources.yml
│   │   │   ├── raw_azure_customers.sql
│   │   │   ├── raw_azure_maintenance.sql
│   │   │   ├── raw_azure_shipments.sql
│   │   │   ├── raw_azure_vehicles.sql
│   │   │   ├── raw_telematics_data.sql
│   │   │   ├── raw_traffic_data.sql
│   │   │   └── raw_weather_data.sql
│   │   └── staging
│   │       ├── stg_customers.sql
│   │       ├── stg_maintenance_logs.sql
│   │       ├── stg_shipments.sql
│   │       ├── stg_traffic_conditions.sql
│   │       ├── stg_vehicle_telemetry.sql
│   │       ├── stg_vehicles.sql
│   │       └── stg_weather_conditions.sql
│   ├── packages.yml
│   ├── profiles.yml
│   └── tests
│       ├── business_rules
│       │   ├── test_customer_segmentation.sql
│       │   ├── test_kpi_calculations.sql
│       │   ├── test_maintenance_intervals.sql
│       │   └── test_shipment_status_logic.sql
│       ├── data_quality
│       │   ├── test_delivery_time_realistic.sql
│       │   ├── test_fuel_efficiency_reasonable.sql
│       │   ├── test_route_distance_positive.sql
│       │   └── test_vehicle_capacity_not_exceeded.sql
│       └── referential_integrity
│           ├── test_fact_dimension_relationships.sql
│           └── test_foreign_key_constraints.sql
├── fivetran
│   ├── connectors
│   │   ├── azure-sql-connector-config.json
│   │   ├── telematics-webhook-config.json
│   │   ├── traffic-api-connector-config.json
│   │   └── weather-api-connector-config.json
│   └── monitoring
│       ├── connector_health_check.sql
│       ├── data_quality_alerts.sql
│       └── sync_monitoring.sql
├── scripts
│   └── deployment
│       └── deploy_dbt_models.sh
├── snowflake
│   ├── optimization
│   │   ├── automated_tasks.sql
│   │   ├── clustering_keys.sql
│   │   ├── cost_monitoring.sql
│   │   ├── emergency_procedures.sql
│   │   └── performance_tuning.sql
│   ├── setup
│   │   ├── 01_database_setup.sql
│   │   ├── 02_schema_creation.sql
│   │   ├── 03_warehouse_configuration.sql
│   │   ├── 04_user_roles_permissions.sql
│   │   └── 05_resource_monitors.sql
│   ├── tables
│   │   ├── dimensions
│   │   │   ├── dim_customer.sql
│   │   │   ├── dim_date.sql
│   │   │   ├── dim_location.sql
│   │   │   ├── dim_route.sql
│   │   │   ├── dim_traffic_conditions.sql
│   │   │   ├── dim_vehicle.sql
│   │   │   ├── dim_vehicle_maintenance.sql
│   │   │   └── dim_weather.sql
│   │   └── facts
│   │       ├── fact_route_conditions.sql
│   │       ├── fact_route_performance.sql
│   │       ├── fact_shipments.sql
│   │       ├── fact_vehicle_telemetry.sql
│   │       └── fact_vehicle_utilization.sql
│   └── views
│       ├── business_intelligence
│       │   ├── view_ai_recommendations.sql
│       │   └── view_performance_dashboard.sql
│       ├── cost_optimization
│       │   ├── view_monthly_cost_forecast.sql
│       │   ├── view_query_cost_analysis.sql
│       │   ├── view_resource_monitor_usage.sql
│       │   └── view_warehouse_cost_analysis.sql
│       ├── ml_features
│       │   ├── view_customer_behavior_segments.sql
│       │   ├── view_haul_segmentation.sql
│       │   ├── view_ml_feature_store.sql
│       │   ├── view_predictive_maintenance_features.sql
│       │   └── view_route_optimization_features.sql
│       ├── monitoring
│       │   ├── view_data_freshness_monitoring.sql
│       │   ├── view_data_quality_summary.sql
│       │   ├── view_dbt_run_results.sql
│       │   └── view_fivetran_sync_status.sql
│       └── rolling_analytics
│           ├── view_customer_behavior_rolling.sql
│           ├── view_maintenance_rolling_indicators.sql
│           ├── view_operational_performance_rolling.sql
│           └── view_route_performance_rolling.sql
└── source-database
    ├── copy_into.sql
    ├── create_tables.sql
    └── insert_into.sql