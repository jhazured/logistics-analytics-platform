-- =====================================================
-- Resource Monitors for Logistics Analytics Platform
-- Cost Management and Query Governance
-- =====================================================

-- Set context
USE ROLE ACCOUNTADMIN;

-- =====================================================
-- 1. ACCOUNT-LEVEL RESOURCE MONITOR
-- =====================================================

CREATE OR REPLACE RESOURCE MONITOR LOGISTICS_ACCOUNT_MONITOR
WITH 
    CREDIT_QUOTA = 10000                    -- Monthly credit limit
    FREQUENCY = MONTHLY
    START_TIMESTAMP = DATE_TRUNC('MONTH', CURRENT_DATE())
    TRIGGERS 
        ON 50 PERCENT DO NOTIFY             -- Alert at 50% usage
        ON 75 PERCENT DO NOTIFY             -- Warning at 75% usage
        ON 90 PERCENT DO SUSPEND_IMMEDIATE  -- Emergency stop at 90%
        ON 100 PERCENT DO SUSPEND_IMMEDIATE -- Hard limit at 100%
    NOTIFY_USERS = ('data_team@example.com');

-- Apply to account
ALTER ACCOUNT SET RESOURCE_MONITOR = LOGISTICS_ACCOUNT_MONITOR;

-- =====================================================
-- 2. WAREHOUSE-SPECIFIC RESOURCE MONITORS
-- =====================================================

-- X-Small Warehouse Monitor (Development and automation)
CREATE OR REPLACE RESOURCE MONITOR RM_COMPUTE_WH_XS
WITH 
    CREDIT_QUOTA = 100                      -- 1% of account quota
    FREQUENCY = MONTHLY
    START_TIMESTAMP = DATE_TRUNC('MONTH', CURRENT_DATE())
    TRIGGERS 
        ON 80 PERCENT DO NOTIFY             -- Warning at 80%
        ON 100 PERCENT DO SUSPEND           -- Suspend at limit
    NOTIFY_USERS = ('data_team@example.com');

-- Small Warehouse Monitor (Staging and testing)
CREATE OR REPLACE RESOURCE MONITOR RM_COMPUTE_WH_SMALL
WITH 
    CREDIT_QUOTA = 500                      -- 5% of account quota
    FREQUENCY = MONTHLY
    START_TIMESTAMP = DATE_TRUNC('MONTH', CURRENT_DATE())
    TRIGGERS 
        ON 80 PERCENT DO NOTIFY             -- Warning at 80%
        ON 100 PERCENT DO SUSPEND           -- Suspend at limit
    NOTIFY_USERS = ('data_team@example.com');

-- Medium Warehouse Monitor (Production analytics and ML training)
CREATE OR REPLACE RESOURCE MONITOR RM_COMPUTE_WH_MEDIUM
WITH 
    CREDIT_QUOTA = 1000                     -- 10% of account quota
    FREQUENCY = MONTHLY
    START_TIMESTAMP = DATE_TRUNC('MONTH', CURRENT_DATE())
    TRIGGERS 
        ON 80 PERCENT DO NOTIFY             -- Warning at 80%
        ON 100 PERCENT DO SUSPEND           -- Suspend at limit
    NOTIFY_USERS = ('data_team@example.com');

-- Large Warehouse Monitor (Heavy ML model training)
CREATE OR REPLACE RESOURCE MONITOR RM_COMPUTE_WH_LARGE
WITH 
    CREDIT_QUOTA = 2000                     -- 20% of account quota
    FREQUENCY = MONTHLY
    START_TIMESTAMP = DATE_TRUNC('MONTH', CURRENT_DATE())
    TRIGGERS 
        ON 80 PERCENT DO NOTIFY             -- Warning at 80%
        ON 100 PERCENT DO SUSPEND           -- Suspend at limit
    NOTIFY_USERS = ('data_team@example.com');

-- X-Large Warehouse Monitor (Intensive ML workloads)
CREATE OR REPLACE RESOURCE MONITOR RM_COMPUTE_WH_XLARGE
WITH 
    CREDIT_QUOTA = 5000                     -- 50% of account quota
    FREQUENCY = MONTHLY
    START_TIMESTAMP = DATE_TRUNC('MONTH', CURRENT_DATE())
    TRIGGERS 
        ON 80 PERCENT DO NOTIFY             -- Warning at 80%
        ON 100 PERCENT DO SUSPEND           -- Suspend at limit
    NOTIFY_USERS = ('data_team@example.com');

-- Legacy Warehouse Monitors (for backward compatibility)
CREATE OR REPLACE RESOURCE MONITOR WH_LOADING_MONITOR
WITH 
    CREDIT_QUOTA = 3000                     -- 30% of account quota
    FREQUENCY = MONTHLY
    START_TIMESTAMP = DATE_TRUNC('MONTH', CURRENT_DATE())
    TRIGGERS 
        ON 70 PERCENT DO NOTIFY             -- Allow higher usage for batch jobs
        ON 85 PERCENT DO NOTIFY             -- Escalated warning
        ON 95 PERCENT DO SUSPEND            -- Suspend near limit
        ON 100 PERCENT DO SUSPEND_IMMEDIATE -- Hard limit
    NOTIFY_USERS = ('data_team@example.com');

CREATE OR REPLACE RESOURCE MONITOR WH_ANALYTICS_MONITOR
WITH 
    CREDIT_QUOTA = 4000                     -- 40% of account quota
    FREQUENCY = MONTHLY
    START_TIMESTAMP = DATE_TRUNC('MONTH', CURRENT_DATE())
    TRIGGERS 
        ON 60 PERCENT DO NOTIFY             -- Early warning
        ON 80 PERCENT DO NOTIFY             -- Mid-month warning
        ON 90 PERCENT DO SUSPEND            -- Suspend at 90%
        ON 100 PERCENT DO SUSPEND_IMMEDIATE -- Hard stop
    NOTIFY_USERS = ('data_team@example.com');

CREATE OR REPLACE RESOURCE MONITOR WH_ML_MONITOR
WITH 
    CREDIT_QUOTA = 3000                     -- 30% of account quota
    FREQUENCY = MONTHLY
    START_TIMESTAMP = DATE_TRUNC('MONTH', CURRENT_DATE())
    TRIGGERS 
        ON 50 PERCENT DO NOTIFY             -- Early notification for expensive ML workloads
        ON 70 PERCENT DO NOTIFY             -- Escalated warning
        ON 85 PERCENT DO SUSPEND            -- Suspend before hard limit
        ON 100 PERCENT DO SUSPEND_IMMEDIATE -- Hard stop
    NOTIFY_USERS = ('data_team@example.com');

-- =====================================================
-- 3. DAILY/WEEKLY RESOURCE MONITORS FOR TIGHT CONTROL
-- =====================================================

-- Daily Monitor for Critical Operations
CREATE OR REPLACE RESOURCE MONITOR DAILY_OPERATIONS_MONITOR
WITH 
    CREDIT_QUOTA = 500                      -- Daily limit across all warehouses
    FREQUENCY = DAILY
    START_TIMESTAMP = DATE_TRUNC('DAY', CURRENT_DATE())
    TRIGGERS 
        ON 75 PERCENT DO NOTIFY             -- Warning at 75%
        ON 90 PERCENT DO SUSPEND            -- Suspend at 90%
        ON 100 PERCENT DO SUSPEND_IMMEDIATE -- Hard daily limit
    NOTIFY_USERS = ('data_team@example.com');

-- Weekly Monitor for Project-based Work
CREATE OR REPLACE RESOURCE MONITOR WEEKLY_PROJECT_MONITOR
WITH 
    CREDIT_QUOTA = 2500                     -- Weekly project allowance
    FREQUENCY = WEEKLY
    START_TIMESTAMP = DATE_TRUNC('WEEK', CURRENT_DATE())
    TRIGGERS 
        ON 60 PERCENT DO NOTIFY             -- Mid-week check
        ON 80 PERCENT DO NOTIFY             -- Late week warning
        ON 95 PERCENT DO SUSPEND_IMMEDIATE  -- Prevent weekend overruns
    NOTIFY_USERS = ('data_team@example.com');

-- =====================================================
-- 4. QUERY-LEVEL GOVERNANCE (RESOURCE MONITORS + TIMEOUTS)
-- =====================================================

-- Set query timeout parameters by warehouse
ALTER WAREHOUSE COMPUTE_WH_XS SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 3600     -- 1 hour for development
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 300; -- 5 min queue timeout

ALTER WAREHOUSE COMPUTE_WH_SMALL SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 7200     -- 2 hours for staging
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 600; -- 10 min queue timeout

ALTER WAREHOUSE COMPUTE_WH_MEDIUM SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 10800    -- 3 hours for production analytics
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 900; -- 15 min queue timeout

ALTER WAREHOUSE COMPUTE_WH_LARGE SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 14400    -- 4 hours for ML training
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 1200; -- 20 min queue timeout

ALTER WAREHOUSE COMPUTE_WH_XLARGE SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 18000    -- 5 hours for intensive ML workloads
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 1800; -- 30 min queue timeout

-- Legacy warehouse timeouts
ALTER WAREHOUSE WH_LOADING SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 10800    -- 3 hours for data loads
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 300; -- 5 min queue timeout

ALTER WAREHOUSE WH_ANALYTICS SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 7200     -- 2 hours max for analytics
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 600; -- 10 min queue timeout

ALTER WAREHOUSE WH_ML SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 14400    -- 4 hours for ML training
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 1200; -- 20 min queue timeout

-- =====================================================
-- 5. ALERTING AND NOTIFICATION SETUP
-- =====================================================

-- Create notification integration (requires ACCOUNTADMIN)
CREATE OR REPLACE NOTIFICATION INTEGRATION logistics_email_integration
TYPE = EMAIL
ENABLED = TRUE
ALLOWED_RECIPIENTS = (
    'data_team@example.com',
    'finance_team@example.com',
    'ml_team@example.com'
);

-- =====================================================
-- 6. RESOURCE MONITOR COMMENTS
-- =====================================================

COMMENT ON RESOURCE MONITOR LOGISTICS_ACCOUNT_MONITOR IS 'Account-level resource monitor for overall cost control';
COMMENT ON RESOURCE MONITOR RM_COMPUTE_WH_XS IS 'Resource monitor for X-Small warehouse (development and automation)';
COMMENT ON RESOURCE MONITOR RM_COMPUTE_WH_SMALL IS 'Resource monitor for Small warehouse (staging and testing)';
COMMENT ON RESOURCE MONITOR RM_COMPUTE_WH_MEDIUM IS 'Resource monitor for Medium warehouse (production analytics and ML training)';
COMMENT ON RESOURCE MONITOR RM_COMPUTE_WH_LARGE IS 'Resource monitor for Large warehouse (heavy ML model training)';
COMMENT ON RESOURCE MONITOR RM_COMPUTE_WH_XLARGE IS 'Resource monitor for X-Large warehouse (intensive ML workloads)';
COMMENT ON RESOURCE MONITOR DAILY_OPERATIONS_MONITOR IS 'Daily resource monitor for critical operations';
COMMENT ON RESOURCE MONITOR WEEKLY_PROJECT_MONITOR IS 'Weekly resource monitor for project-based work';