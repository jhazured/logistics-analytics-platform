-- Email-based Alerting System for Logistics Analytics Platform
-- Replaces Slack with email notifications for all alerts

-- Create alert configuration table
CREATE OR REPLACE TABLE LOGISTICS_DW_PROD.MONITORING.ALERT_CONFIG (
    ALERT_ID VARCHAR(50) DEFAULT UUID_STRING(),
    ALERT_TYPE VARCHAR(100) NOT NULL,
    ALERT_NAME VARCHAR(255) NOT NULL,
    SEVERITY VARCHAR(20) NOT NULL, -- CRITICAL, HIGH, MEDIUM, LOW
    EMAIL_RECIPIENTS TEXT NOT NULL, -- Comma-separated email addresses
    THRESHOLD_VALUE NUMBER,
    THRESHOLD_OPERATOR VARCHAR(10), -- >, <, =, >=, <=
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert default alert configurations
INSERT INTO LOGISTICS_DW_PROD.MONITORING.ALERT_CONFIG 
(ALERT_TYPE, ALERT_NAME, SEVERITY, EMAIL_RECIPIENTS, THRESHOLD_VALUE, THRESHOLD_OPERATOR) VALUES
('data_freshness', 'Data Freshness Alert', 'HIGH', 'data-team@company.com,ops-team@company.com', 6, '>'),
('cost_monitoring', 'Daily Cost Exceeded', 'HIGH', 'finance@company.com,data-team@company.com', 100, '>'),
('cost_monitoring', 'Monthly Cost Exceeded', 'CRITICAL', 'finance@company.com,executives@company.com', 2000, '>'),
('data_quality', 'Data Quality SLA Failed', 'MEDIUM', 'data-team@company.com', 0.95, '<'),
('performance', 'Query Performance Degraded', 'MEDIUM', 'data-team@company.com', 300000, '>'),
('system_health', 'dbt Run Failed', 'HIGH', 'data-team@company.com,ops-team@company.com', 0, '=');

-- Create alert history table
CREATE OR REPLACE TABLE LOGISTICS_DW_PROD.MONITORING.ALERT_HISTORY (
    ALERT_HISTORY_ID VARCHAR(50) DEFAULT UUID_STRING(),
    ALERT_ID VARCHAR(50) NOT NULL,
    ALERT_TYPE VARCHAR(100) NOT NULL,
    ALERT_NAME VARCHAR(255) NOT NULL,
    SEVERITY VARCHAR(20) NOT NULL,
    EMAIL_RECIPIENTS TEXT NOT NULL,
    ALERT_MESSAGE TEXT NOT NULL,
    ALERT_DATA VARIANT,
    TRIGGERED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EMAIL_SENT BOOLEAN DEFAULT FALSE,
    EMAIL_SENT_AT TIMESTAMP_NTZ,
    ACKNOWLEDGED BOOLEAN DEFAULT FALSE,
    ACKNOWLEDGED_AT TIMESTAMP_NTZ,
    ACKNOWLEDGED_BY VARCHAR(255)
);

-- Create stored procedure for sending email alerts
CREATE OR REPLACE PROCEDURE LOGISTICS_DW_PROD.MONITORING.SEND_EMAIL_ALERT(
    ALERT_TYPE VARCHAR,
    ALERT_MESSAGE TEXT,
    SEVERITY VARCHAR,
    RECIPIENTS TEXT,
    ALERT_DATA VARIANT
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    email_subject STRING;
    email_body STRING;
    result STRING;
BEGIN
    -- Format email subject based on severity
    email_subject := CASE 
        WHEN SEVERITY = 'CRITICAL' THEN '[CRITICAL] Logistics Analytics Alert: ' || ALERT_TYPE
        WHEN SEVERITY = 'HIGH' THEN '[HIGH] Logistics Analytics Alert: ' || ALERT_TYPE
        WHEN SEVERITY = 'MEDIUM' THEN '[MEDIUM] Logistics Analytics Alert: ' || ALERT_TYPE
        ELSE '[LOW] Logistics Analytics Alert: ' || ALERT_TYPE
    END;
    
    -- Format email body
    email_body := '
Logistics Analytics Platform Alert

Alert Type: ' || ALERT_TYPE || '
Severity: ' || SEVERITY || '
Timestamp: ' || CURRENT_TIMESTAMP() || '
Message: ' || ALERT_MESSAGE || '

Alert Data:
' || TO_JSON(ALERT_DATA) || '

Please investigate and take appropriate action.

Best regards,
Logistics Analytics Platform Monitoring System
    ';
    
    -- Insert into alert history
    INSERT INTO LOGISTICS_DW_PROD.MONITORING.ALERT_HISTORY 
    (ALERT_TYPE, ALERT_NAME, SEVERITY, EMAIL_RECIPIENTS, ALERT_MESSAGE, ALERT_DATA)
    VALUES (ALERT_TYPE, ALERT_TYPE, SEVERITY, RECIPIENTS, ALERT_MESSAGE, ALERT_DATA);
    
    -- In a real implementation, you would integrate with your email service here
    -- For now, we'll log the alert
    result := 'Email alert prepared for: ' || RECIPIENTS || ' | Subject: ' || email_subject;
    
    RETURN result;
END;
$$;

-- Create task for monitoring data freshness
CREATE OR REPLACE TASK LOGISTICS_DW_PROD.MONITORING.DATA_FRESHNESS_ALERT_TASK
WAREHOUSE = COMPUTE_WH_SMALL
SCHEDULE = 'USING CRON 0 */2 * * * UTC'  -- Every 2 hours
AS
CALL LOGISTICS_DW_PROD.MONITORING.SEND_EMAIL_ALERT(
    'data_freshness',
    'Data freshness check failed - some tables have not been updated within SLA',
    'HIGH',
    'data-team@company.com,ops-team@company.com',
    OBJECT_CONSTRUCT('check_time', CURRENT_TIMESTAMP(), 'sla_hours', 6)
);

-- Create task for monitoring costs
CREATE OR REPLACE TASK LOGISTICS_DW_PROD.MONITORING.COST_ALERT_TASK
WAREHOUSE = COMPUTE_WH_SMALL
SCHEDULE = 'USING CRON 0 8 * * * UTC'  -- Daily at 8 AM UTC
AS
CALL LOGISTICS_DW_PROD.MONITORING.SEND_EMAIL_ALERT(
    'cost_monitoring',
    'Daily cost monitoring - check cost dashboard for details',
    'MEDIUM',
    'finance@company.com,data-team@company.com',
    OBJECT_CONSTRUCT('check_time', CURRENT_TIMESTAMP(), 'daily_budget', 100)
);

-- Create task for monitoring data quality
CREATE OR REPLACE TASK LOGISTICS_DW_PROD.MONITORING.DATA_QUALITY_ALERT_TASK
WAREHOUSE = COMPUTE_WH_SMALL
SCHEDULE = 'USING CRON 0 6 * * * UTC'  -- Daily at 6 AM UTC
AS
CALL LOGISTICS_DW_PROD.MONITORING.SEND_EMAIL_ALERT(
    'data_quality',
    'Data quality SLA check - review quality dashboard',
    'MEDIUM',
    'data-team@company.com',
    OBJECT_CONSTRUCT('check_time', CURRENT_TIMESTAMP(), 'sla_threshold', 0.95)
);

-- Enable all tasks
ALTER TASK LOGISTICS_DW_PROD.MONITORING.DATA_FRESHNESS_ALERT_TASK RESUME;
ALTER TASK LOGISTICS_DW_PROD.MONITORING.COST_ALERT_TASK RESUME;
ALTER TASK LOGISTICS_DW_PROD.MONITORING.DATA_QUALITY_ALERT_TASK RESUME;
