-- Configure account-level audit settings
-- Enable account-level audit logging
ALTER ACCOUNT SET LOG_LEVEL = 'INFO';
ALTER ACCOUNT SET LOG_RETENTION_DAYS = 90;

-- Enable query history logging
ALTER ACCOUNT SET QUERY_HISTORY_RETENTION_DAYS = 90;

-- Enable access history logging
ALTER ACCOUNT SET ACCESS_HISTORY_RETENTION_DAYS = 90;
