# Logistics Analytics Platform - File Index

This document contains raw GitHub URLs for all files in the logistics-analytics-platform project.

## Root Files
- [LICENSE](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/LICENSE)
- [README.md](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/README.md)
- [requirements.txt](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/requirements.txt)
- [.gitignore](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/.gitignore)

## GitHub Workflows
- [dbt-docs.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/.github/workflows/dbt-docs.yml)
- [dbt.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/.github/workflows/dbt.yml)
- [dbt_ci_cd.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/.github/workflows/dbt_ci_cd.yml)

## Data Generation
- [generate_sample_data.py](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/data/generate_sample_data.py)

## dbt Configuration
- [dbt_project.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/dbt_project.yml)
- [exposures.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/exposures.yml)
- [packages.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/packages.yml)
- [profiles.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/profiles.yml)

## dbt Macros
- [advanced_logistics_analytics.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/advanced_logistics_analytics.sql)
- [cost_calculations.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/cost_calculations.sql)
- [data_quality_checks.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/data_quality_checks.sql)
- [date_helpers.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/date_helpers.sql)
- [logistics_calculations.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/logistics_calculations.sql)
- [predictive_maintenance.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/predictive_maintenance.sql)
- [rolling_windows.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/rolling_windows.sql)
- [stream_processing.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/stream_processing.sql)
- [traffic_calculations.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/traffic_calculations.sql)
- [weather_impact_calculations.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/macros/weather_impact_calculations.sql)

## dbt Analytics Models
- [ai_recommendations.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/analytics/ai_recommendations.sql)
- [data_freshness_monitoring.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/analytics/data_freshness_monitoring.sql)
- [executive_dashboard_trending.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/analytics/executive_dashboard_trending.sql)
- [performance_dashboard.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/analytics/performance_dashboard.sql)
- [schema.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/analytics/schema.yml)
- [sustainability_metrics.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/analytics/sustainability_metrics.sql)

## dbt Dimension Models
- [dim_customer.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/dim_customer.sql)
- [dim_date.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/dim_date.sql)
- [dim_location.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/dim_location.sql)
- [dim_route.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/dim_route.sql)
- [dim_traffic_conditions.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/dim_traffic_conditions.sql)
- [dim_vehicle.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/dim_vehicle.sql)
- [dim_vehicle_maintenance.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/dim_vehicle_maintenance.sql)
- [dim_weather.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/dim_weather.sql)
- [schema.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/dimensions/schema.yml)

## dbt Fact Models
- [fact_route_conditions.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/facts/fact_route_conditions.sql)
- [fact_route_performance.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/facts/fact_route_performance.sql)
- [fact_shipments.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/facts/fact_shipments.sql)
- [fact_vehicle_telemetry.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/facts/fact_vehicle_telemetry.sql)
- [fact_vehicle_utilization.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/facts/fact_vehicle_utilization.sql)
- [schema.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/facts/schema.yml)

## dbt ML Features
- [ml_customer_behavior_rolling.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/ml_features/ml_customer_behavior_rolling.sql)
- [ml_customer_behavior_segments.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/ml_features/ml_customer_behavior_segments.sql)
- [ml_feature_store.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/ml_features/ml_feature_store.sql)
- [ml_haul_segmentation.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/ml_features/ml_haul_segmentation.sql)
- [ml_maintenance_rolling_indicators.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/ml_features/ml_maintenance_rolling_indicators.sql)
- [ml_operational_performance_rolling.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/ml_features/ml_operational_performance_rolling.sql)
- [ml_predictive_maintenance_features.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/ml_features/ml_predictive_maintenance_features.sql)
- [ml_real_time_scoring.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/ml_features/ml_real_time_scoring.sql)
- [ml_route_optimization_features.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/ml_features/ml_route_optimization_features.sql)
- [ml_route_performance_rolling.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/marts/ml_features/ml_route_performance_rolling.sql)

## dbt Raw Models
- [_sources.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/_sources.yml)
- [raw_azure_customers.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/raw_azure_customers.sql)
- [raw_azure_maintenance.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/raw_azure_maintenance.sql)
- [raw_azure_shipments.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/raw_azure_shipments.sql)
- [raw_azure_vehicles.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/raw_azure_vehicles.sql)
- [raw_telematics_data.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/raw_telematics_data.sql)
- [raw_traffic_data.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/raw_traffic_data.sql)
- [raw_weather_data.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/raw/raw_weather_data.sql)

## dbt Staging Models
- [schema.yml](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/schema.yml)
- [stg_customers.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/stg_customers.sql)
- [stg_maintenance_logs.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/stg_maintenance_logs.sql)
- [stg_routes.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/stg_routes.sql)
- [stg_shipments.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/stg_shipments.sql)
- [stg_traffic_conditions.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/stg_traffic_conditions.sql)
- [stg_vehicle_telemetry.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/stg_vehicle_telemetry.sql)
- [stg_vehicles.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/stg_vehicles.sql)
- [stg_weather_conditions.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/models/staging/stg_weather_conditions.sql)

## dbt Snapshots
- [customers_snapshot.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/snapshots/customers_snapshot.sql)
- [vehicles_snapshot.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/snapshots/vehicles_snapshot.sql)

## dbt Business Rules Tests
- [test_customer_segmentation.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/business_rules/test_customer_segmentation.sql)
- [test_customer_tier_consistency.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/dbt/tests/business_rules/test_customer_tier_consistency.sql)
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
- [real_time_kpis.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/streaming/real_time_kpis.sql)
- [task_management.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/streaming/task_management.sql)

## Snowflake Tables - Dimensions
- [dim_customer.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/dim_customer.sql)
- [dim_date.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/dim_date.sql)
- [dim_location.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/dim_location.sql)
- [dim_route.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/dim_route.sql)
- [dim_traffic_conditions.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/dim_traffic_conditions.sql)
- [dim_vehicle.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/dim_vehicle.sql)
- [dim_vehicle_maintenance.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/dim_vehicle_maintenance.sql)
- [dim_weather.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/dimensions/dim_weather.sql)

## Snowflake Tables - Facts
- [fact_route_conditions.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/facts/fact_route_conditions.sql)
- [fact_route_performance.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/facts/fact_route_performance.sql)
- [fact_shipments.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/facts/fact_shipments.sql)
- [fact_vehicle_telemetry.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/facts/fact_vehicle_telemetry.sql)
- [fact_vehicle_utilization.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/tables/facts/fact_vehicle_utilization.sql)

## Snowflake Views - Business Intelligence
- [view_ai_recommendations.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/business_intelligence/view_ai_recommendations.sql)
- [view_performance_dashboard.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/business_intelligence/view_performance_dashboard.sql)

## Snowflake Views - Cost Optimization
- [view_monthly_cost_forecast.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/cost_optimization/view_monthly_cost_forecast.sql)
- [view_query_cost_analysis.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/cost_optimization/view_query_cost_analysis.sql)
- [view_resource_monitor_usage.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/cost_optimization/view_resource_monitor_usage.sql)
- [view_warehouse_cost_analysis.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/cost_optimization/view_warehouse_cost_analysis.sql)

## Snowflake Views - ML Features
- [view_customer_behavior_segments.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/ml_features/view_customer_behavior_segments.sql)
- [view_haul_segmentation.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/ml_features/view_haul_segmentation.sql)
- [view_ml_feature_store.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/ml_features/view_ml_feature_store.sql)
- [view_predictive_maintenance_features.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/ml_features/view_predictive_maintenance_features.sql)
- [view_route_optimization_features.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/ml_features/view_route_optimization_features.sql)

## Snowflake Views - Monitoring
- [view_data_freshness_monitoring.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/monitoring/view_data_freshness_monitoring.sql)
- [view_data_quality_summary.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/monitoring/view_data_quality_summary.sql)
- [view_dbt_run_results.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/monitoring/view_dbt_run_results.sql)
- [view_fivetran_sync_status.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/monitoring/view_fivetran_sync_status.sql)

## Snowflake Views - Rolling Analytics
- [view_customer_behavior_rolling.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/rolling_analytics/view_customer_behavior_rolling.sql)
- [view_customer_behaviour_rolling.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/rolling_analytics/view_customer_behaviour_rolling.sql)
- [view_maintenance_rolling_indicators.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/rolling_analytics/view_maintenance_rolling_indicators.sql)
- [view_operational_performance_rolling.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/rolling_analytics/view_operational_performance_rolling.sql)
- [view_route_performance_rolling.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/snowflake/views/rolling_analytics/view_route_performance_rolling.sql)

## Source Database
- [copy_into.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/source-database/copy_into.sql)
- [create_tables.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/source-database/create_tables.sql)
- [insert_into.sql](https://raw.githubusercontent.com/jhazured/logistics-analytics-platform/refs/heads/main/source-database/insert_into.sql)

---

**Total Files:** 150+ files across the entire logistics analytics platform

**Repository:** [jhazured/logistics-analytics-platform](https://github.com/jhazured/logistics-analytics-platform)

**Branch:** main

**Last Updated:** Generated automatically from project structure
