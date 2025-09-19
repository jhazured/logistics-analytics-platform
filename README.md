Project structure

logistics-analytics-platform/
├── README.md
├── docs/
│   ├── architecture-overview.md
│   ├── data-model-documentation.md
│   ├── migration-strategy.md
│   └── business-requirements.md
│
├── data-generation/
│   ├── generate_sample_data.py
│   ├── azure_sql_setup.sql
│   ├── populate_source_database.py
│   └── requirements.txt
│
├── source-database/
│   ├── schema/
│   │   ├── create_tables.sql
│   │   ├── create_indexes.sql
│   │   ├── create_views.sql
│   │   └── legacy_stored_procedures.sql
│   ├── sample-data/
│   │   ├── shipments_sample.csv
│   │   ├── vehicles_sample.csv
│   │   ├── customers_sample.csv
│   │   └── maintenance_logs_sample.csv
│   └── queries/
│       ├── legacy_reports.sql
│       └── data_profiling.sql
│
├── fivetran/
│   ├── connectors/
│   │   ├── azure-sql-connector-config.json
│   │   ├── weather-api-connector-config.json
│   │   ├── traffic-api-connector-config.json
│   │   └── telematics-webhook-config.json
│   ├── transformations/
│   │   ├── basic_data_cleaning.sql
│   │   └── staging_prep.sql
│   └── monitoring/
│       ├── connector_health_check.sql
│       ├── sync_monitoring.sql
│       └── data_quality_alerts.sql
│
├── snowflake/
│   ├── setup/
│   │   ├── 01_database_setup.sql
│   │   ├── 02_schema_creation.sql
│   │   ├── 03_warehouse_configuration.sql
│   │   ├── 04_user_roles_permissions.sql
│   │   └── 05_resource_monitors.sql
│   ├── tables/
│   │   ├── dimensions/
│   │   │   ├── dim_customer.sql
│   │   │   ├── dim_location.sql
│   │   │   ├── dim_vehicle.sql
│   │   │   ├── dim_route.sql
│   │   │   ├── dim_date.sql
│   │   │   ├── dim_weather.sql
│   │   │   ├── dim_traffic_conditions.sql
│   │   │   └── dim_vehicle_maintenance.sql
│   │   └── facts/
│   │       ├── fact_shipments.sql
│   │       ├── fact_vehicle_telemetry.sql
│   │       ├── fact_route_conditions.sql
│   │       ├── fact_vehicle_utilization.sql
│   │       └── fact_route_performance.sql
│   ├── views/
│   │   ├── ml_features/
│   │   │   ├── view_haul_segmentation.sql
│   │   │   ├── view_customer_behavior_segments.sql
│   │   │   ├── view_route_optimization_features.sql
│   │   │   ├── view_predictive_maintenance_features.sql
│   │   │   └── view_ml_feature_store.sql
│   │   ├── rolling_analytics/
│   │   │   ├── view_operational_performance_rolling.sql
│   │   │   ├── view_maintenance_rolling_indicators.sql
│   │   │   ├── view_customer_behavior_rolling.sql
│   │   │   └── view_route_performance_rolling.sql
│   │   ├── business_intelligence/
│   │   │   ├── view_performance_dashboard.sql
│   │   │   ├── view_executive_dashboard_trending.sql
│   │   │   └── view_ai_recommendations.sql
│   │   ├── monitoring/
│   │   │   ├── view_data_freshness_monitoring.sql
│   │   │   ├── view_fivetran_sync_status.sql
│   │   │   ├── view_dbt_run_results.sql
│   │   │   └── view_model_monitoring.sql
│   │   ├── cost_optimization/
│   │   │   ├── view_warehouse_cost_analysis.sql
│   │   │   └── view_storage_cost_optimization.sql
│   │   └── operational/
│   │       ├── view_migration_validation.sql
│   │       ├── view_business_continuity.sql
│   │       ├── view_sustainability_metrics.sql
│   │       └── view_real_time_scoring.sql
│   ├── optimization/
│   │   ├── clustering_keys.sql
│   │   ├── automated_tasks.sql
│   │   ├── performance_tuning.sql
│   │   └── cost_monitoring.sql
│   ├── migration/
│   │   ├── pre_migration_validation.sql
│   │   ├── data_comparison_queries.sql
│   │   ├── post_migration_validation.sql
│   │   └── cutover_procedures.sql
│   └── security/
│       ├── row_level_security.sql
│       ├── column_masking.sql
│       └── data_sharing.sql
│
dbt/
├── dbt_project.yml
├── profiles.yml
├── packages.yml
│
├── models/
│   ├── raw/
│   │   ├── _sources.yml
│   │   ├── raw_azure_shipments.sql
│   │   ├── raw_azure_vehicles.sql
│   │   ├── raw_azure_customers.sql
│   │   ├── raw_azure_maintenance.sql
│   │   ├── raw_weather_data.sql
│   │   ├── raw_traffic_data.sql
│   │   └── raw_telematics_data.sql
│   │
│   ├── staging/
│   │   ├── stg_shipments.sql
│   │   ├── stg_vehicles.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_maintenance_logs.sql
│   │   ├── stg_weather_conditions.sql
│   │   ├── stg_traffic_conditions.sql
│   │   └── stg_vehicle_telemetry.sql
│   │
│   ├── marts/
│   │   ├── dimensions/
│   │   │   ├── dim_customer.sql
│   │   │   ├── dim_location.sql
│   │   │   ├── dim_vehicle.sql
│   │   │   ├── dim_route.sql
│   │   │   ├── dim_date.sql
│   │   │   ├── dim_weather.sql
│   │   │   ├── dim_traffic_conditions.sql
│   │   │   └── dim_vehicle_maintenance.sql
│   │   │
│   │   ├── facts/
│   │   │   ├── fact_shipments.sql
│   │   │   ├── fact_vehicle_telemetry.sql
│   │   │   ├── fact_route_conditions.sql
│   │   │   ├── fact_vehicle_utilization.sql
│   │   │   └── fact_route_performance.sql
│   │   │
│   │   ├── ml_features/
│   │   │   ├── ml_haul_segmentation.sql
│   │   │   ├── ml_customer_behavior_segments.sql
│   │   │   ├── ml_route_optimization_features.sql
│   │   │   ├── ml_predictive_maintenance_features.sql
│   │   │   ├── ml_operational_performance_rolling.sql
│   │   │   ├── ml_maintenance_rolling_indicators.sql
│   │   │   ├── ml_customer_behavior_rolling.sql
│   │   │   ├── ml_route_performance_rolling.sql
│   │   │   ├── ml_feature_store.sql
│   │   │   └── ml_real_time_scoring.sql
│   │   │
│   │   └── analytics/
│   │       ├── performance_dashboard.sql
│   │       ├── executive_dashboard_trending.sql
│   │       ├── ai_recommendations.sql
│   │       ├── data_freshness_monitoring.sql
│   │       ├── model_monitoring.sql
│   │       ├── warehouse_cost_analysis.sql
│   │       ├── migration_validation.sql
│   │       ├── business_continuity.sql
│   │       └── sustainability_metrics.sql
│   │
│   └── _models.yml
│
├── macros/
│   ├── logistics_calculations.sql
│   ├── rolling_windows.sql
│   ├── data_quality_checks.sql
│   ├── cost_calculations.sql
│   └── date_helpers.sql
│
├── tests/
│   ├── data_quality/
│   │   ├── test_delivery_time_realistic.sql
│   │   ├── test_vehicle_capacity_not_exceeded.sql
│   │   ├── test_route_distance_positive.sql
│   │   └── test_fuel_efficiency_reasonable.sql
│   │
│   ├── business_rules/
│   │   ├── test_shipment_status_logic.sql
│   │   ├── test_maintenance_intervals.sql
│   │   ├── test_customer_segmentation.sql
│   │   └── test_kpi_calculations.sql
│   │
│   └── referential_integrity/
│       ├── test_fact_dimension_relationships.sql
│       └── test_foreign_key_constraints.sql
│
├── seeds/
│   ├── branch_locations.csv
│   ├── vehicle_specifications.csv
│   ├── service_levels.csv
│   └── holiday_calendar.csv
│
├── snapshots/
│   ├── customers_snapshot.sql
│   ├── vehicles_snapshot.sql
│   └── rates_snapshot.sql
│
├── analyses/
│   ├── migration_impact_analysis.sql
│   ├── performance_benchmarking.sql
│   └── cost_benefit_analysis.sql
│
└── docs/
    ├── model_documentation.md
    ├── kpi_definitions.md
    ├── data_lineage.md
    └── testing_strategy.md
│
├── scripts/
│   ├── deployment/
│   │   ├── deploy_snowflake_objects.sh
│   │   ├── deploy_dbt_models.sh
│   │   ├── setup_fivetran_connectors.py
│   │   └── end_to_end_deployment.sh
│   │
│   ├── monitoring/
│   │   ├── check_data_quality.py
│   │   ├── monitor_pipeline_health.py
│   │   ├── cost_monitoring.py
│   │   └── alert_notifications.py
│   │
│   ├── utilities/
│   │   ├── data_profiling.py
│   │   ├── schema_comparison.py
│   │   ├── performance_testing.sql
│   │   └── backup_procedures.sh
│   │
│   └── migration/
│       ├── pre_migration_checklist.py
│       ├── migration_execution.py
│       ├── post_migration_validation.py
│       └── rollback_procedures.py
│
├── infrastructure/
│   ├── terraform/
│   │   ├── main.tf
│   │   ├── azure_sql_database.tf
│   │   ├── snowflake_resources.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   │
│   └── docker/
│       ├── Dockerfile
│       ├── docker-compose.yml
│       └── requirements.txt
│
├── tests/
│   ├── integration/
│   │   ├── test_end_to_end_pipeline.py
│   │   ├── test_data_consistency.py
│   │   └── test_performance_benchmarks.py
│   │
│   └── unit/
│       ├── test_dbt_macros.py
│       ├── test_data_quality_functions.py
│       └── test_utility_scripts.py
│
├── .github/
│   ├── workflows/
│   │   ├── dbt_ci_cd.yml
│   │   ├── data_quality_checks.yml
│   │   └── deployment_pipeline.yml
│   │
│   └── pull_request_template.md
│
├── .gitignore
├── requirements.txt
└── LICENSE