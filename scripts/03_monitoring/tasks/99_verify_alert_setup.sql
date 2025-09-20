-- Verify alert system setup

-- Check if alert tables exist
SELECT 
    'TABLE' as object_type,
    table_name,
    table_schema,
    created_on
FROM INFORMATION_SCHEMA.TABLES 
WHERE table_schema = 'MONITORING'
AND table_name LIKE '%ALERT%'
ORDER BY created_on DESC;

-- Check if monitoring tasks exist and are enabled
SELECT 
    'TASK' as object_type,
    task_name,
    warehouse_name,
    schedule,
    state,
    created_on
FROM INFORMATION_SCHEMA.TASKS 
WHERE task_schema = 'MONITORING'
AND task_name LIKE '%alert%'
ORDER BY created_on DESC;

-- Check notification integration
SELECT 
    'NOTIFICATION_INTEGRATION' as object_type,
    name,
    type,
    enabled,
    created_on
FROM INFORMATION_SCHEMA.INTEGRATIONS 
WHERE type = 'EMAIL'
AND name = 'EMAIL_ALERT_INTEGRATION';

-- Check procedures
SELECT 
    'PROCEDURE' as object_type,
    procedure_name,
    procedure_schema,
    created_on
FROM INFORMATION_SCHEMA.PROCEDURES 
WHERE procedure_schema = 'MONITORING'
AND procedure_name LIKE '%alert%'
ORDER BY created_on DESC;

-- Test alert system with a sample alert
INSERT INTO LOGISTICS_DW_PROD.MONITORING.SYSTEM_ALERTS (
    alert_type, severity, component, message
) VALUES (
    'TEST_ALERT', 'LOW', 'VERIFICATION', 'Test alert to verify system setup'
);

-- Check if test alert was created
SELECT 
    alert_id,
    alert_type,
    severity,
    component,
    message,
    alert_timestamp
FROM LOGISTICS_DW_PROD.MONITORING.SYSTEM_ALERTS 
WHERE alert_type = 'TEST_ALERT'
ORDER BY alert_timestamp DESC
LIMIT 1;
