-- Set up audit policies and procedures

-- Create procedure to collect audit data
CREATE OR REPLACE PROCEDURE SP_collect_audit_data()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {
        // Collect account events from query history
        var query = `
            INSERT INTO AUDIT_DB.AUDIT_LOGS.ACCOUNT_EVENTS (
                EVENT_TYPE, USER_NAME, CLIENT_IP, CLIENT_APPLICATION, 
                QUERY_ID, QUERY_TEXT, DATABASE_NAME, SCHEMA_NAME, 
                SUCCESS, ERROR_CODE, ERROR_MESSAGE, ROWS_AFFECTED, 
                CREDITS_USED, EXECUTION_TIME_MS
            )
            SELECT 
                'QUERY_EXECUTION' as EVENT_TYPE,
                USER_NAME,
                CLIENT_IP,
                CLIENT_APPLICATION_NAME,
                QUERY_ID,
                QUERY_TEXT,
                DATABASE_NAME,
                SCHEMA_NAME,
                SUCCESS,
                ERROR_CODE,
                ERROR_MESSAGE,
                ROWS_PRODUCED,
                CREDITS_USED_CLOUD_SERVICES,
                TOTAL_ELAPSED_TIME
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
            WHERE START_TIME >= CURRENT_DATE() - 1
            AND QUERY_ID NOT IN (SELECT QUERY_ID FROM AUDIT_DB.AUDIT_LOGS.ACCOUNT_EVENTS WHERE EVENT_TYPE = 'QUERY_EXECUTION')
        `;
        
        var stmt = snowflake.createStatement({sqlText: query});
        stmt.execute();
        
        return "Audit data collection completed successfully";
    } catch (err) {
        return "Error collecting audit data: " + err.message;
    }
$$;

-- Create task to run audit collection daily
CREATE OR REPLACE TASK TSK_audit_data_collection_task
WAREHOUSE = COMPUTE_WH_XS
SCHEDULE = 'USING CRON 0 2 * * * UTC'  -- Daily at 2 AM UTC
COMMENT = 'Daily audit data collection task'
AS
CALL SP_collect_audit_data();

-- Enable the task
ALTER TASK TSK_audit_data_collection_task RESUME;
