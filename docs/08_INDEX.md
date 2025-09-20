# Logistics Analytics Platform - File Index

This document contains raw GitHub URLs for all files in the logistics-analytics-platform project.

## Root Files
- [LICENSE](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/LICENSE)
- [README.md](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/README.md)
- [INCREMENTAL_LOADING_STRATEGY.md](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/INCREMENTAL_LOADING_STRATEGY.md)
- [requirements.txt](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/requirements.txt)
- [.gitignore](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/.gitignore)

## GitHub Workflows
- [dbt-docs.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/.github/workflows/dbt-docs.yml)
- [dbt.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/.github/workflows/dbt.yml)
- [dbt_ci_cd.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/.github/workflows/dbt_ci_cd.yml)

## Data Generation
- [generate_sample_data.py](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/data/generate_sample_data.py)

## Fivetran Monitoring
- [connector_health_check.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/fivetran/monitoring/connector_health_check.sql)
- [data_quality_alerts.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/fivetran/monitoring/data_quality_alerts.sql)
- [sync_monitoring.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/fivetran/monitoring/sync_monitoring.sql)

## dbt Configuration
- [dbt_project.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/dbt_project.yml)
- [exposures.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/exposures.yml)
- [packages.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/packages.yml)
- [profiles.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/profiles.yml)

## dbt Macros
- [aggregations.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/aggregations.sql)
- [business_logic.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/business_logic.sql)
- [cost_calculations.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/cost_calculations.sql)
- [data_types.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/data_types.sql)
- [date_time.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/date_time.sql)
- [error_handling.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/error_handling.sql)
- [post_hooks.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/post_hooks.sql)
- [rolling_windows.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/rolling_windows.sql)

## dbt Analytics Models
- [vw_ai_recommendations.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/analytics/vw_ai_recommendations.sql)
- [vw_consolidated_dashboard.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/analytics/vw_consolidated_dashboard.sql)
- [vw_data_freshness_monitoring.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/analytics/vw_data_freshness_monitoring.sql)
- [vw_data_lineage.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/analytics/vw_data_lineage.sql)
- [vw_data_quality_sla.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/analytics/vw_data_quality_sla.sql)
- [schema.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/analytics/schema.yml)
- [vw_sustainability_metrics.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/analytics/vw_sustainability_metrics.sql)

## dbt Dimension Models
- [tbl_dim_customer.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/tbl_dim_customer.sql)
- [tbl_dim_date.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/tbl_dim_date.sql)
- [tbl_dim_location.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/tbl_dim_location.sql)
- [tbl_dim_route.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/tbl_dim_route.sql)
- [tbl_dim_traffic_conditions.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/tbl_dim_traffic_conditions.sql)
- [tbl_dim_vehicle.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/tbl_dim_vehicle.sql)
- [tbl_dim_vehicle_maintenance.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/tbl_dim_vehicle_maintenance.sql)
- [tbl_dim_weather.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/tbl_dim_weather.sql)
- [schema.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/schema.yml)

## dbt Fact Models
- [tbl_fact_route_conditions.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/facts/tbl_fact_route_conditions.sql)
- [tbl_fact_route_performance.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/facts/tbl_fact_route_performance.sql)
- [tbl_fact_shipments.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/facts/tbl_fact_shipments.sql)
- [tbl_fact_vehicle_telemetry.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/facts/tbl_fact_vehicle_telemetry.sql)
- [tbl_fact_vehicle_utilization.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/facts/tbl_fact_vehicle_utilization.sql)
- [schema.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/facts/schema.yml)

## dbt ML Features
- [tbl_ml_consolidated_feature_store.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/ml_features/tbl_ml_consolidated_feature_store.sql)
- [tbl_ml_rolling_analytics.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/ml_features/tbl_ml_rolling_analytics.sql)
- [tbl_ml_maintenance_features.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/ml_features/tbl_ml_maintenance_features.sql)
- [tbl_ml_customer_behavior_segments.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/ml_features/tbl_ml_customer_behavior_segments.sql)
- [tbl_ml_haul_segmentation.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/ml_features/tbl_ml_haul_segmentation.sql)

## dbt ML Serving Models
- [vw_ml_real_time_customer_features.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/ml_serving/vw_ml_real_time_customer_features.sql)
- [vw_ml_real_time_vehicle_features.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/ml_serving/vw_ml_real_time_vehicle_features.sql)

## dbt Raw Models
- [_sources.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/_sources.yml)
- [tbl_raw_azure_customers.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/tbl_raw_azure_customers.sql)
- [tbl_raw_azure_maintenance.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/tbl_raw_azure_maintenance.sql)
- [tbl_raw_azure_shipments.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/tbl_raw_azure_shipments.sql)
- [tbl_raw_azure_vehicles.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/tbl_raw_azure_vehicles.sql)
- [tbl_raw_telematics_data.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/tbl_raw_telematics_data.sql)
- [tbl_raw_traffic_data.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/tbl_raw_traffic_data.sql)
- [tbl_raw_weather_data.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/tbl_raw_weather_data.sql)

## dbt Staging Models
- [schema.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/schema.yml)
- [tbl_stg_customers.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/tbl_stg_customers.sql)
- [tbl_stg_maintenance_logs.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/tbl_stg_maintenance_logs.sql)
- [tbl_stg_routes.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/tbl_stg_routes.sql)
- [tbl_stg_shipments.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/tbl_stg_shipments.sql)
- [tbl_stg_traffic_conditions.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/tbl_stg_traffic_conditions.sql)
- [tbl_stg_vehicle_telemetry.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/tbl_stg_vehicle_telemetry.sql)
- [tbl_stg_vehicles.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/tbl_stg_vehicles.sql)
- [tbl_stg_weather_conditions.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/tbl_stg_weather_conditions.sql)

## dbt Snapshots
- [customers_snapshot.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/snapshots/customers_snapshot.sql)
- [vehicles_snapshot.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/snapshots/vehicles_snapshot.sql)
- [routes_snapshot.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/snapshots/routes_snapshot.sql)
- [locations_snapshot.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/snapshots/locations_snapshot.sql)

## Scripts
- [configure_environment.sh](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/scripts/setup/configure_environment.sh)
- [deploy_dbt_models.sh](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/scripts/deployment/deploy_dbt_models.sh)

## dbt Business Rules Tests
- [test_analytics_view_consistency.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/business_rules/test_analytics_view_consistency.sql)
- [test_customer_tier_validation.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/business_rules/test_customer_tier_validation.sql)
- [test_kpi_calculations.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/business_rules/test_kpi_calculations.sql)
- [test_maintenance_intervals.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/business_rules/test_maintenance_intervals.sql)
- [test_maintenance_schedule_compliance.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/business_rules/test_maintenance_schedule_compliance.sql)
- [test_route_efficiency_bounds.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/business_rules/test_route_efficiency_bounds.sql)
- [test_seasonal_demand_patterns.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/business_rules/test_seasonal_demand_patterns.sql)
- [test_shipment_status_logic.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/business_rules/test_shipment_status_logic.sql)

## dbt Data Quality Tests
- [test_cost_reasonableness.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/data_quality/test_cost_reasonableness.sql)
- [test_data_freshness_monitoring.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/data_quality/test_data_freshness_monitoring.sql)
- [test_delivery_time_realistic.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/data_quality/test_delivery_time_realistic.sql)
- [test_fuel_efficiency_reasonable.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/data_quality/test_fuel_efficiency_reasonable.sql)
- [test_referential_integrity_shipments.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/data_quality/test_referential_integrity_shipments.sql)
- [test_route_distance_positive.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/data_quality/test_route_distance_positive.sql)
- [test_vehicle_capacity_not_exceeded.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/data_quality/test_vehicle_capacity_not_exceeded.sql)

## dbt Referential Integrity Tests
- [test_fact_dimension_relationships.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/referential_integrity/test_fact_dimension_relationships.sql)
- [test_foreign_key_constraints.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/referential_integrity/test_foreign_key_constraints.sql)

## Fivetran Connectors
- [azure-sql-connector-config.json](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/fivetran/connectors/azure-sql-connector-config.json)
- [telematics-webhook-config.json](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/fivetran/connectors/telematics-webhook-config.json)
- [traffic-api-connector-config.json](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/fivetran/connectors/traffic-api-connector-config.json)
- [weather-api-connector-config.json](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/fivetran/connectors/weather-api-connector-config.json)

## Fivetran Monitoring
- [connector_health_check.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/fivetran/monitoring/connector_health_check.sql)
- [data_quality_alerts.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/fivetran/monitoring/data_quality_alerts.sql)
- [sync_monitoring.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/fivetran/monitoring/sync_monitoring.sql)

## Scripts
- [deploy_dbt_models.sh](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/scripts/deployment/deploy_dbt_models.sh)
- [configure_environment.sh](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/scripts/setup/configure_environment.sh)

## Snowflake Optimization
- [automated_tasks.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/optimization/automated_tasks.sql)
- [clustering_keys.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/optimization/clustering_keys.sql)
- [cost_monitoring.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/optimization/cost_monitoring.sql)
- [emergency_procedures.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/optimization/emergency_procedures.sql)
- [performance_tuning.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/optimization/performance_tuning.sql)

## Snowflake Security
- [audit_logging.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/security/audit_logging.sql)
- [data_classification.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/security/data_classification.sql)
- [data_masking_policies.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/security/data_masking_policies.sql)
- [row_level_security.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/security/row_level_security.sql)

## Snowflake Setup
- [01_database_setup.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/setup/01_database_setup.sql)
- [02_schema_creation.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/setup/02_schema_creation.sql)
- [03_warehouse_configuration.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/setup/03_warehouse_configuration.sql)
- [04_user_roles_permissions.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/setup/04_user_roles_permissions.sql)
- [05_resource_monitors.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/setup/05_resource_monitors.sql)

## Snowflake Streaming
- [alert_system.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/streaming/alert_system.sql)
- [create_streams.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/streaming/create_streams.sql)
- [create_tasks.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/streaming/create_tasks.sql)
- [deploy_streams_and_tasks.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/streaming/deploy_streams_and_tasks.sql)
- [email_alerting_system.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/streaming/email_alerting_system.sql)
- [real_time_kpis.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/streaming/real_time_kpis.sql)
- [task_management.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/streaming/task_management.sql)

## Snowflake Tables - Dimensions
- [tbl_dim_customer.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/tbl_dim_customer.sql)
- [tbl_dim_date.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/tbl_dim_date.sql)
- [tbl_dim_location.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/tbl_dim_location.sql)
- [tbl_dim_route.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/tbl_dim_route.sql)
- [tbl_dim_traffic_conditions.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/tbl_dim_traffic_conditions.sql)
- [tbl_dim_vehicle.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/tbl_dim_vehicle.sql)
- [tbl_dim_vehicle_maintenance.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/tbl_dim_vehicle_maintenance.sql)
- [tbl_dim_weather.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/tbl_dim_weather.sql)

## Snowflake Tables - Facts
- [tbl_fact_route_conditions.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/facts/tbl_fact_route_conditions.sql)
- [tbl_fact_route_performance.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/facts/tbl_fact_route_performance.sql)
- [tbl_fact_shipments.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/facts/tbl_fact_shipments.sql)
- [tbl_fact_vehicle_telemetry.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/facts/tbl_fact_vehicle_telemetry.sql)
- [tbl_fact_vehicle_utilization.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/facts/tbl_fact_vehicle_utilization.sql)

## Snowflake Views - Cost Optimization
- [vw_monthly_cost_forecast.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/cost_optimization/vw_monthly_cost_forecast.sql)
- [vw_query_cost_analysis.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/cost_optimization/vw_query_cost_analysis.sql)
- [vw_resource_monitor_usage.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/cost_optimization/vw_resource_monitor_usage.sql)
- [vw_warehouse_cost_analysis.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/cost_optimization/vw_warehouse_cost_analysis.sql)

## Snowflake Views - Monitoring
- [vw_cost_monitoring.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/monitoring/vw_cost_monitoring.sql)
- [vw_data_quality_summary.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/monitoring/vw_data_quality_summary.sql)
- [vw_dbt_run_results.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/monitoring/vw_dbt_run_results.sql)
- [vw_fivetran_sync_status.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/monitoring/vw_fivetran_sync_status.sql)
- [vw_performance_monitoring.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/monitoring/vw_performance_monitoring.sql)

## Snowflake ML Objects - Model Registry
- [tbl_ml_model_registry.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/ml_objects/model_registry/tbl_ml_model_registry.sql)

## Snowflake ML Objects - Serving Views
- [vw_ml_real_time_features.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/ml_objects/serving_views/vw_ml_real_time_features.sql)
- [vw_ml_real_time_maintenance.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/ml_objects/serving_views/vw_ml_real_time_maintenance.sql)

## Snowflake ML Objects - Monitoring
- [vw_ml_feature_monitoring.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/ml_objects/monitoring/vw_ml_feature_monitoring.sql)

## Source Database
- [copy_into.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/source-database/copy_into.sql)
- [create_tables.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/source-database/create_tables.sql)
- [insert_into.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/source-database/insert_into.sql)

---

**Total Files:** 150+ files across the entire logistics analytics platform

**Repository:** [jhazured/logistics-analytics-platform](https://github.com/jhazured/logistics-analytics-platform)

**Branch:** main

**Last Updated:** Generated automatically from project structure
