-- Verify streams and tasks deployment

-- Check streams
SELECT 
    'STREAM' as object_type,
    stream_name,
    source_database,
    source_schema,
    source_table,
    created_on
FROM INFORMATION_SCHEMA.STREAMS 
WHERE stream_schema = 'MARTS'
ORDER BY created_on DESC;

-- Check tasks
SELECT 
    'TASK' as object_type,
    task_name,
    warehouse_name,
    schedule,
    state,
    created_on
FROM INFORMATION_SCHEMA.TASKS 
WHERE task_schema = 'MARTS'
ORDER BY created_on DESC;

-- Check monitoring tables
SELECT 
    'TABLE' as object_type,
    table_name,
    table_schema,
    created_on
FROM INFORMATION_SCHEMA.TABLES 
WHERE table_schema = 'MONITORING'
AND table_name LIKE '%REAL_TIME%'
ORDER BY created_on DESC;
