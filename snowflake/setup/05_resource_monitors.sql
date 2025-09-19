-- =====================================================
-- Smart Logistics Analytics Platform - Snowflake Resource Monitors
-- Cost Management and Query Governance
-- =====================================================

-- Set context
USE ROLE ACCOUNTADMIN;
USE DATABASE LOGISTICS_DW;

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
    NOTIFY_USERS = ('jharkeris@hotmail.com');

-- Apply to account
ALTER ACCOUNT SET RESOURCE_MONITOR = LOGISTICS_ACCOUNT_MONITOR;

-- =====================================================
-- 2. WAREHOUSE-SPECIFIC RESOURCE MONITORS (3 Warehouses)
-- =====================================================

-- Loading Warehouse Monitor (Data ingestion and ETL operations)
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
    NOTIFY_USERS = ('jharkeris@hotmail.com');

ALTER WAREHOUSE WH_LOADING SET RESOURCE_MONITOR = WH_LOADING_MONITOR;

-- Analytics Warehouse Monitor (BI, reporting, and general analytics)
CREATE OR REPLACE RESOURCE MONITOR WH_ANALYTICS_MONITOR
WITH 
    CREDIT_QUOTA = 4000                     -- 40% of account quota (largest allocation)
    FREQUENCY = MONTHLY
    START_TIMESTAMP = DATE_TRUNC('MONTH', CURRENT_DATE())
    TRIGGERS 
        ON 60 PERCENT DO NOTIFY             -- Early warning
        ON 80 PERCENT DO NOTIFY             -- Mid-month warning
        ON 90 PERCENT DO SUSPEND            -- Suspend at 90%
        ON 100 PERCENT DO SUSPEND_IMMEDIATE -- Hard stop
    NOTIFY_USERS = ('jharkeris@hotmail.com');

ALTER WAREHOUSE WH_ANALYTICS SET RESOURCE_MONITOR = WH_ANALYTICS_MONITOR;

-- ML Warehouse Monitor (Machine Learning training and scoring)
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
    NOTIFY_USERS = ('jharkeris@hotmail.com');

ALTER WAREHOUSE WH_ML SET RESOURCE_MONITOR = WH_ML_MONITOR;

-- =====================================================
-- 3. DAILY/WEEKLY RESOURCE MONITORS FOR TIGHT CONTROL
-- =====================================================

-- Daily Monitor for Critical Operations (Prevents daily spikes)
CREATE OR REPLACE RESOURCE MONITOR DAILY_OPERATIONS_MONITOR
WITH 
    CREDIT_QUOTA = 500                      -- Daily limit across all warehouses
    FREQUENCY = DAILY
    START_TIMESTAMP = DATE_TRUNC('DAY', CURRENT_DATE())
    TRIGGERS 
        ON 75 PERCENT DO NOTIFY             -- Warning at 75%
        ON 90 PERCENT DO SUSPEND            -- Suspend at 90%
        ON 100 PERCENT DO SUSPEND_IMMEDIATE -- Hard daily limit
    NOTIFY_USERS = ('jharkeris@hotmail.com');

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
    NOTIFY_USERS = ('jharkeris@hotmail.com');

-- =====================================================
-- 4. QUERY-LEVEL GOVERNANCE (RESOURCE MONITORS + TIMEOUTS)
-- =====================================================

-- Set query timeout parameters by warehouse (3 warehouses only)
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
-- 6. ALERTING AND NOTIFICATION SETUP
-- =====================================================

-- Create notification integration (requires ACCOUNTADMIN)
CREATE OR REPLACE NOTIFICATION INTEGRATION logistics_email_integration
TYPE = EMAIL
ENABLED = TRUE
ALLOWED_RECIPIENTS = (
    'jharkeris@hotmail.com'
);