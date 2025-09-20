-- =====================================================
-- Unified Setup Script for Logistics Analytics Platform
-- =====================================================
-- This script provides flexible setup options based on environment variables
-- Supports both minimal (build-and-run) and complete (production) deployments
-- 
-- Configuration via environment variables:
--   SF_DATABASE: Target database name (default: LOGISTICS_DW_DEV)
--   SF_SCHEMA: Default schema (default: ANALYTICS)

-- =====================================================
-- 1. DATABASE SETUP
-- =====================================================
SELECT 'Setting up database: ' || IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV') as status;

-- Create database
CREATE DATABASE IF NOT EXISTS IDENTIFIER(IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV'));
USE DATABASE IDENTIFIER(IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV'));

-- Set database properties
ALTER DATABASE IDENTIFIER(IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV')) SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- Grant usage to admin role
GRANT USAGE ON DATABASE IDENTIFIER(IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV')) TO ROLE ACCOUNTADMIN;

-- Add database comment
COMMENT ON DATABASE IDENTIFIER(IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV')) IS 'Logistics analytics platform database - Environment: ' || IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV');

-- =====================================================
-- 2. SCHEMA CREATION
-- =====================================================
SELECT 'Creating schemas...' as status;

-- Core schemas (always created)
CREATE SCHEMA IF NOT EXISTS RAW;                    -- Raw data ingestion
CREATE SCHEMA IF NOT EXISTS STAGING;                -- Data cleaning
CREATE SCHEMA IF NOT EXISTS MARTS;                  -- Business logic
CREATE SCHEMA IF NOT EXISTS ANALYTICS;              -- Analytics views

-- Additional schemas
CREATE SCHEMA IF NOT EXISTS ML_FEATURES;            -- ML feature engineering
CREATE SCHEMA IF NOT EXISTS MONITORING;             -- System monitoring
CREATE SCHEMA IF NOT EXISTS SNAPSHOTS;              -- Change data capture
CREATE SCHEMA IF NOT EXISTS ML_OBJECTS;             -- ML model registry
CREATE SCHEMA IF NOT EXISTS GOVERNANCE;             -- Data governance
CREATE SCHEMA IF NOT EXISTS PERFORMANCE;            -- Performance optimization
CREATE SCHEMA IF NOT EXISTS SECURITY;               -- Security and access control

-- Set default schema
USE SCHEMA IDENTIFIER(IFNULL($SF_SCHEMA, 'ANALYTICS'));

-- =====================================================
-- 3. WAREHOUSE CONFIGURATION
-- =====================================================
SELECT 'Configuring warehouses...' as status;

-- Create warehouses
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH_XS
WITH WAREHOUSE_SIZE = 'X-SMALL'
     AUTO_SUSPEND = 60
     AUTO_RESUME = TRUE
     INITIALLY_SUSPENDED = TRUE
     COMMENT = 'Small warehouse for development and light workloads';

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH_SMALL
WITH WAREHOUSE_SIZE = 'SMALL'
     AUTO_SUSPEND = 60
     AUTO_RESUME = TRUE
     INITIALLY_SUSPENDED = TRUE
     COMMENT = 'Small warehouse for staging and testing';

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH_MEDIUM
WITH WAREHOUSE_SIZE = 'MEDIUM'
     AUTO_SUSPEND = 60
     AUTO_RESUME = TRUE
     INITIALLY_SUSPENDED = TRUE
     COMMENT = 'Medium warehouse for production analytics and ML training';

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH_LARGE
WITH WAREHOUSE_SIZE = 'LARGE'
     AUTO_SUSPEND = 60
     AUTO_RESUME = TRUE
     INITIALLY_SUSPENDED = TRUE
     COMMENT = 'Large warehouse for heavy ML model training and data science';

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH_XLARGE
WITH WAREHOUSE_SIZE = 'X-LARGE'
     AUTO_SUSPEND = 60
     AUTO_RESUME = TRUE
     INITIALLY_SUSPENDED = TRUE
     COMMENT = 'Extra large warehouse for intensive ML workloads';

-- =====================================================
-- 4. USER ROLES
-- =====================================================
SELECT 'Creating roles...' as status;

-- Create essential roles
CREATE ROLE IF NOT EXISTS DATA_ENGINEER;
CREATE ROLE IF NOT EXISTS DATA_ANALYST;
CREATE ROLE IF NOT EXISTS DATA_SCIENTIST;
CREATE ROLE IF NOT EXISTS ML_ENGINEER;
CREATE ROLE IF NOT EXISTS BUSINESS_USER;
CREATE ROLE IF NOT EXISTS DATA_STEWARD;
CREATE ROLE IF NOT EXISTS SECURITY_ADMIN;
CREATE ROLE IF NOT EXISTS DBT_DEV_ROLE;
CREATE ROLE IF NOT EXISTS DBT_STAGING_ROLE;
CREATE ROLE IF NOT EXISTS DBT_PROD_ROLE;

-- =====================================================
-- 5. RESOURCE MONITORS
-- =====================================================
SELECT 'Setting up resource monitors...' as status;

-- Create resource monitors
CREATE RESOURCE MONITOR IF NOT EXISTS RM_COMPUTE_WH_XS
WITH CREDIT_QUOTA = 100
     FREQUENCY = 'MONTHLY'
     START_TIMESTAMP = 'IMMEDIATELY'
     TRIGGERS
     ON 80 PERCENT DO NOTIFY
     ON 100 PERCENT DO SUSPEND;

CREATE RESOURCE MONITOR IF NOT EXISTS RM_COMPUTE_WH_SMALL
WITH CREDIT_QUOTA = 500
     FREQUENCY = 'MONTHLY'
     START_TIMESTAMP = 'IMMEDIATELY'
     TRIGGERS
     ON 80 PERCENT DO NOTIFY
     ON 100 PERCENT DO SUSPEND;

CREATE RESOURCE MONITOR IF NOT EXISTS RM_COMPUTE_WH_MEDIUM
WITH CREDIT_QUOTA = 1000
     FREQUENCY = 'MONTHLY'
     START_TIMESTAMP = 'IMMEDIATELY'
     TRIGGERS
     ON 80 PERCENT DO NOTIFY
     ON 100 PERCENT DO SUSPEND;

CREATE RESOURCE MONITOR IF NOT EXISTS RM_COMPUTE_WH_LARGE
WITH CREDIT_QUOTA = 2000
     FREQUENCY = 'MONTHLY'
     START_TIMESTAMP = 'IMMEDIATELY'
     TRIGGERS
     ON 80 PERCENT DO NOTIFY
     ON 100 PERCENT DO SUSPEND;

CREATE RESOURCE MONITOR IF NOT EXISTS RM_COMPUTE_WH_XLARGE
WITH CREDIT_QUOTA = 5000
     FREQUENCY = 'MONTHLY'
     START_TIMESTAMP = 'IMMEDIATELY'
     TRIGGERS
     ON 80 PERCENT DO NOTIFY
     ON 100 PERCENT DO SUSPEND;

-- Assign resource monitors to warehouses
ALTER WAREHOUSE COMPUTE_WH_XS SET RESOURCE_MONITOR = RM_COMPUTE_WH_XS;
ALTER WAREHOUSE COMPUTE_WH_SMALL SET RESOURCE_MONITOR = RM_COMPUTE_WH_SMALL;
ALTER WAREHOUSE COMPUTE_WH_MEDIUM SET RESOURCE_MONITOR = RM_COMPUTE_WH_MEDIUM;
ALTER WAREHOUSE COMPUTE_WH_LARGE SET RESOURCE_MONITOR = RM_COMPUTE_WH_LARGE;
ALTER WAREHOUSE COMPUTE_WH_XLARGE SET RESOURCE_MONITOR = RM_COMPUTE_WH_XLARGE;

-- =====================================================
-- 6. VERIFICATION
-- =====================================================
SELECT 'Verifying setup...' as status;

-- Show configuration used
SELECT 
    'CONFIGURATION' as info_type,
    IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV') as database_name,
    IFNULL($SF_SCHEMA, 'ANALYTICS') as default_schema;

-- Check databases
SELECT 'DATABASES' as object_type, database_name, created 
FROM information_schema.databases 
WHERE database_name = IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV');

-- Check schemas
SELECT 'SCHEMAS' as object_type, schema_name, schema_catalog
FROM information_schema.schemata 
WHERE schema_catalog = IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV')
ORDER BY schema_name;

-- Check roles
SELECT 'ROLES' as object_type, role_name, created_on
FROM information_schema.roles 
WHERE role_name IN ('DATA_ENGINEER', 'DATA_ANALYST', 'DATA_SCIENTIST', 'ML_ENGINEER', 'BUSINESS_USER', 'DATA_STEWARD', 'SECURITY_ADMIN', 'DBT_DEV_ROLE', 'DBT_STAGING_ROLE', 'DBT_PROD_ROLE')
ORDER BY role_name;

-- Check warehouses
SELECT 'WAREHOUSES' as object_type, warehouse_name, warehouse_size, auto_suspend, auto_resume
FROM information_schema.warehouses 
WHERE warehouse_name LIKE 'COMPUTE_WH_%'
ORDER BY warehouse_name;

-- Check resource monitors
SELECT 'RESOURCE_MONITORS' as object_type, name, credit_quota, frequency
FROM information_schema.resource_monitors 
WHERE name LIKE 'RM_%'
ORDER BY name;

-- =====================================================
-- 7. COMPLETION MESSAGE
-- =====================================================
SELECT 
    'SETUP_COMPLETE' as status,
    'Unified setup completed successfully' as message,
    IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV') as database_name,
    'Ready for deployment with full flexibility' as next_steps;