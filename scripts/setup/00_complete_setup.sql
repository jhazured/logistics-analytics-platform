-- =====================================================
-- Complete Setup Script for Logistics Analytics Platform
-- =====================================================
-- This script sets up the complete Snowflake environment
-- Run this script as ACCOUNTADMIN to create all databases,
-- schemas, roles, warehouses, and resource monitors

-- =====================================================
-- 1. DATABASE SETUP
-- =====================================================
-- Creates databases for all environments (prod, dev, staging)

\echo 'Creating databases...'
\i 01_database_setup.sql

-- =====================================================
-- 2. SCHEMA CREATION
-- =====================================================
-- Creates all schemas across all environments

\echo 'Creating schemas...'
\i 02_schema_creation.sql

-- =====================================================
-- 3. WAREHOUSE CONFIGURATION
-- =====================================================
-- Creates and configures warehouses for different workloads

\echo 'Configuring warehouses...'
\i 03_warehouse_configuration.sql

-- =====================================================
-- 4. USER ROLES AND PERMISSIONS
-- =====================================================
-- Creates roles and sets up permissions

\echo 'Setting up roles and permissions...'
\i 04_user_roles_permissions.sql

-- =====================================================
-- 5. RESOURCE MONITORS
-- =====================================================
-- Creates resource monitors for cost control

\echo 'Setting up resource monitors...'
\i 05_resource_monitors.sql

-- =====================================================
-- 6. VERIFICATION
-- =====================================================
-- Verify that all objects were created successfully

\echo 'Verifying setup...'

-- Check databases
SELECT 'DATABASES' as object_type, database_name, created 
FROM information_schema.databases 
WHERE database_name LIKE 'LOGISTICS_DW%'
ORDER BY database_name;

-- Check schemas
SELECT 'SCHEMAS' as object_type, schema_name, schema_catalog
FROM information_schema.schemata 
WHERE schema_catalog LIKE 'LOGISTICS_DW%'
ORDER BY schema_catalog, schema_name;

-- Check roles
SELECT 'ROLES' as object_type, role_name, created_on
FROM information_schema.roles 
WHERE role_name IN ('DATA_ENGINEER', 'DATA_ANALYST', 'DATA_SCIENTIST', 'ML_ENGINEER', 'BUSINESS_USER', 'DATA_STEWARD', 'SECURITY_ADMIN')
ORDER BY role_name;

-- Check warehouses
SELECT 'WAREHOUSES' as object_type, warehouse_name, warehouse_size, auto_suspend, auto_resume
FROM information_schema.warehouses 
WHERE warehouse_name LIKE 'COMPUTE_WH_%' OR warehouse_name LIKE 'WH_%'
ORDER BY warehouse_name;

-- Check resource monitors
SELECT 'RESOURCE_MONITORS' as object_type, name, credit_quota, frequency
FROM information_schema.resource_monitors 
WHERE name LIKE 'RM_%' OR name LIKE 'WH_%' OR name LIKE 'LOGISTICS_%'
ORDER BY name;

\echo 'Setup complete! All databases, schemas, roles, warehouses, and resource monitors have been created.'
\echo 'Next steps:'
\echo '1. Create users and assign them to appropriate roles'
\echo '2. Configure Fivetran connectors to point to LOGISTICS_DW_PROD.RAW schema'
\echo '3. Run dbt models to create tables and views'
\echo '4. Set up automation framework'
\echo '5. Configure monitoring and alerting'
