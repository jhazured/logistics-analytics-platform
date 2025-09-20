-- Schedule the optimization procedure to run weekly
CREATE OR REPLACE TASK warehouse_optimization_task
WAREHOUSE = WH_ANALYTICS
SCHEDULE = 'USING CRON 0 9 * * 1'  -- 9 AM every Monday
AS 
    CALL optimize_warehouse_sizing();

-- Start the task
ALTER TASK warehouse_optimization_task RESUME;