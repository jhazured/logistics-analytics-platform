-- =====================================================
-- Database Setup for Logistics Analytics Platform
-- =====================================================

-- Create databases for all environments
CREATE DATABASE IF NOT EXISTS LOGISTICS_DW_PROD;
CREATE DATABASE IF NOT EXISTS LOGISTICS_DW_DEV;
CREATE DATABASE IF NOT EXISTS LOGISTICS_DW_STAGING;

-- Set default database
USE DATABASE LOGISTICS_DW_PROD;

-- Grant usage to admin role
GRANT USAGE ON DATABASE LOGISTICS_DW_PROD TO ROLE ACCOUNTADMIN;
GRANT USAGE ON DATABASE LOGISTICS_DW_DEV TO ROLE ACCOUNTADMIN;
GRANT USAGE ON DATABASE LOGISTICS_DW_STAGING TO ROLE ACCOUNTADMIN;

-- Set database properties
ALTER DATABASE LOGISTICS_DW_PROD SET DATA_RETENTION_TIME_IN_DAYS = 7;
ALTER DATABASE LOGISTICS_DW_DEV SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER DATABASE LOGISTICS_DW_STAGING SET DATA_RETENTION_TIME_IN_DAYS = 3;

-- Create comment
COMMENT ON DATABASE LOGISTICS_DW_PROD IS 'Production database for logistics analytics platform';
COMMENT ON DATABASE LOGISTICS_DW_DEV IS 'Development database for logistics analytics platform';
COMMENT ON DATABASE LOGISTICS_DW_STAGING IS 'Staging database for logistics analytics platform';