-- =====================================================
-- Database Setup for Logistics Analytics Platform
-- =====================================================
-- This script creates databases based on environment variables
-- Usage: Set SF_DATABASE environment variable before running
-- Example: export SF_DATABASE="LOGISTICS_DW_DEV" && snowsql -f 01_database_setup.sql

-- Get database name from environment variable or use default
SET DATABASE_NAME = IFNULL($SF_DATABASE, 'LOGISTICS_DW_DEV');

-- Create database
CREATE DATABASE IF NOT EXISTS IDENTIFIER($DATABASE_NAME);

-- Set default database
USE DATABASE IDENTIFIER($DATABASE_NAME);

-- Grant usage to admin role
GRANT USAGE ON DATABASE IDENTIFIER($DATABASE_NAME) TO ROLE ACCOUNTADMIN;

-- Set database properties
ALTER DATABASE IDENTIFIER($DATABASE_NAME) SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- Create comment
COMMENT ON DATABASE IDENTIFIER($DATABASE_NAME) IS 'Database for logistics analytics platform - Environment: ' || $DATABASE_NAME;