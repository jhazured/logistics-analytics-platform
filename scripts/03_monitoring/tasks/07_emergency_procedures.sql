-- Emergency procedure to suspend all warehouses (3 warehouses)
CREATE OR REPLACE PROCEDURE SP_emergency_suspend_all_warehouses()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$
    ALTER WAREHOUSE WH_LOADING SUSPEND;
    ALTER WAREHOUSE WH_ANALYTICS SUSPEND;
    ALTER WAREHOUSE WH_ML SUSPEND;
    
    SELECT 'All 3 warehouses suspended due to emergency procedure';
$;

-- Procedure to resume normal operations (3 warehouses)
CREATE OR REPLACE PROCEDURE SP_resume_normal_operations()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$
    ALTER WAREHOUSE WH_LOADING RESUME;
    ALTER WAREHOUSE WH_ANALYTICS RESUME;
    -- Note: ML warehouse remains suspended until explicitly resumed for safety
    
    SELECT 'Loading and Analytics warehouses resumed - ML requires manual resume';
$;