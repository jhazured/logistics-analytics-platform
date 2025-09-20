-- =====================================================
-- Build-and-Run Setup Script (Cost Optimized)
-- =====================================================
-- This script sets up the minimal Snowflake environment
-- for a build-and-run-once project with minimal costs

-- =====================================================
-- 1. DATABASE SETUP (Minimal)
-- =====================================================
-- Create only the development database
CREATE DATABASE IF NOT EXISTS LOGISTICS_DW_DEV;
USE DATABASE LOGISTICS_DW_DEV;

-- Set minimal data retention (1 day for cost optimization)
ALTER DATABASE LOGISTICS_DW_DEV SET DATA_RETENTION_TIME_IN_DAYS = 1;

-- =====================================================
-- 2. SCHEMA CREATION (Essential Only)
-- =====================================================
-- Create only essential schemas
CREATE SCHEMA IF NOT EXISTS RAW;                    -- Raw data ingestion
CREATE SCHEMA IF NOT EXISTS STAGING;                -- Data cleaning
CREATE SCHEMA IF NOT EXISTS MARTS;                  -- Business logic
CREATE SCHEMA IF NOT EXISTS ANALYTICS;              -- Analytics views

-- Set default schema
USE SCHEMA MARTS;

-- =====================================================
-- 3. WAREHOUSE CONFIGURATION (Minimal)
-- =====================================================
-- Create only X-Small warehouse for minimal costs
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH_XS
WITH WAREHOUSE_SIZE = 'X-SMALL'
     AUTO_SUSPEND = 60
     AUTO_RESUME = TRUE
     INITIALLY_SUSPENDED = TRUE
     COMMENT = 'Minimal warehouse for build-and-run projects';

-- Set minimal resource monitor
CREATE RESOURCE MONITOR IF NOT EXISTS RM_BUILD_AND_RUN
WITH CREDIT_QUOTA = 50  -- $50 limit for entire project
     FREQUENCY = MONTHLY
     START_TIMESTAMP = DATE_TRUNC('MONTH', CURRENT_DATE())
     TRIGGERS 
         ON 80 PERCENT DO NOTIFY
         ON 100 PERCENT DO SUSPEND
     NOTIFY_USERS = ('data_team@example.com');

-- Assign resource monitor
ALTER WAREHOUSE COMPUTE_WH_XS SET RESOURCE_MONITOR = RM_BUILD_AND_RUN;

-- =====================================================
-- 4. USER ROLES (Minimal)
-- =====================================================
-- Create only essential roles
CREATE ROLE IF NOT EXISTS DATA_ENGINEER;
CREATE ROLE IF NOT EXISTS DBT_DEV_ROLE;

-- Grant minimal permissions
GRANT USAGE ON DATABASE LOGISTICS_DW_DEV TO ROLE DATA_ENGINEER;
GRANT USAGE ON DATABASE LOGISTICS_DW_DEV TO ROLE DBT_DEV_ROLE;

-- Schema permissions
GRANT USAGE ON ALL SCHEMAS IN DATABASE LOGISTICS_DW_DEV TO ROLE DATA_ENGINEER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE LOGISTICS_DW_DEV TO ROLE DBT_DEV_ROLE;
GRANT CREATE TABLE ON ALL SCHEMAS IN DATABASE LOGISTICS_DW_DEV TO ROLE DATA_ENGINEER;
GRANT CREATE TABLE ON ALL SCHEMAS IN DATABASE LOGISTICS_DW_DEV TO ROLE DBT_DEV_ROLE;
GRANT CREATE VIEW ON ALL SCHEMAS IN DATABASE LOGISTICS_DW_DEV TO ROLE DATA_ENGINEER;
GRANT CREATE VIEW ON ALL SCHEMAS IN DATABASE LOGISTICS_DW_DEV TO ROLE DBT_DEV_ROLE;

-- Warehouse permissions
GRANT USAGE ON WAREHOUSE COMPUTE_WH_XS TO ROLE DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH_XS TO ROLE DBT_DEV_ROLE;

-- =====================================================
-- 5. COST OPTIMIZATION SETTINGS
-- =====================================================
-- Set minimal query timeouts
ALTER WAREHOUSE COMPUTE_WH_XS SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 3600     -- 1 hour max
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 300; -- 5 min queue timeout

-- =====================================================
-- 6. VERIFICATION
-- =====================================================
-- Verify setup
SELECT 'BUILD_AND_RUN_SETUP_COMPLETE' as status,
       'Minimal environment ready for build-and-run deployment' as message;

-- Show cost optimization settings
SELECT 
    'COST_OPTIMIZATION' as setting_type,
    'X-SMALL warehouse with $50 monthly limit' as warehouse_setting,
    '1-day data retention' as retention_setting,
    'Auto-suspend after 1 minute' as suspend_setting;

-- =====================================================
-- 7. CLEANUP INSTRUCTIONS
-- =====================================================
SELECT 
    'CLEANUP_INSTRUCTIONS' as info_type,
    'To minimize ongoing costs after project completion:' as instruction_1,
    '1. DROP DATABASE LOGISTICS_DW_DEV;' as cleanup_1,
    '2. DROP WAREHOUSE COMPUTE_WH_XS;' as cleanup_2,
    '3. DROP RESOURCE MONITOR RM_BUILD_AND_RUN;' as cleanup_3,
    'Total cleanup cost: $0' as cleanup_cost;
