-- Verify audit logging setup

-- Check if audit database and schema exist
SELECT 
    'DATABASE' as object_type,
    database_name,
    created_on
FROM INFORMATION_SCHEMA.DATABASES 
WHERE database_name = 'AUDIT_DB';

SELECT 
    'SCHEMA' as object_type,
    schema_name,
    created_on
FROM INFORMATION_SCHEMA.SCHEMATA 
WHERE schema_name = 'AUDIT_LOGS' 
AND catalog_name = 'AUDIT_DB';

-- Check if audit tables exist
SELECT 
    'TABLE' as object_type,
    table_name,
    table_schema,
    created_on
FROM INFORMATION_SCHEMA.TABLES 
WHERE table_schema = 'AUDIT_LOGS'
AND table_catalog = 'AUDIT_DB'
ORDER BY created_on DESC;

-- Check if audit task exists and is enabled
SELECT 
    'TASK' as object_type,
    task_name,
    warehouse_name,
    schedule,
    state,
    created_on
FROM INFORMATION_SCHEMA.TASKS 
WHERE task_schema = 'AUDIT_LOGS'
AND task_catalog = 'AUDIT_DB';

-- Check account-level audit settings
SHOW PARAMETERS LIKE 'LOG_LEVEL' IN ACCOUNT;
SHOW PARAMETERS LIKE 'LOG_RETENTION_DAYS' IN ACCOUNT;
