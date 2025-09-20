-- Row Level Security (RLS) Configuration
-- This script sets up row-level security for the logistics analytics platform

-- Enable RLS on fact tables
ALTER TABLE LOGISTICS_DW_PROD.MARTS.TBL_FACT_SHIPMENTS ENABLE ROW ACCESS POLICY;

-- Create RLS policy for shipments based on customer access
CREATE OR REPLACE ROW ACCESS POLICY LOGISTICS_DW_PROD.MARTS.SHIPMENTS_CUSTOMER_ACCESS
AS (customer_id IN (
    SELECT customer_id 
    FROM LOGISTICS_DW_PROD.MARTS.TBL_DIM_CUSTOMER 
    WHERE account_manager = CURRENT_USER()
    OR customer_tier = 'PUBLIC'
));

-- Apply the policy to shipments table
ALTER TABLE LOGISTICS_DW_PROD.MARTS.TBL_FACT_SHIPMENTS 
ADD ROW ACCESS POLICY SHIPMENTS_CUSTOMER_ACCESS ON (customer_id);

-- Create RLS policy for vehicle data based on fleet access
CREATE OR REPLACE ROW ACCESS POLICY LOGISTICS_DW_PROD.MARTS.VEHICLE_FLEET_ACCESS
AS (vehicle_id IN (
    SELECT vehicle_id 
    FROM LOGISTICS_DW_PROD.MARTS.TBL_DIM_VEHICLE 
    WHERE assigned_driver_id = CURRENT_USER()
    OR vehicle_status = 'PUBLIC'
));

-- Apply the policy to vehicle telemetry table
ALTER TABLE LOGISTICS_DW_PROD.MARTS.TBL_FACT_VEHICLE_TELEMETRY 
ADD ROW ACCESS POLICY VEHICLE_FLEET_ACCESS ON (vehicle_id);

-- Create RLS policy for location data based on service area
CREATE OR REPLACE ROW ACCESS POLICY LOGISTICS_DW_PROD.MARTS.LOCATION_SERVICE_AREA_ACCESS
AS (location_id IN (
    SELECT DISTINCT l.location_id 
    FROM LOGISTICS_DW_PROD.MARTS.DIM_LOCATION l
    INNER JOIN LOGISTICS_DW_PROD.MARTS.TBL_FACT_SHIPMENTS s ON l.location_id = s.origin_location_id
    INNER JOIN LOGISTICS_DW_PROD.MARTS.TBL_DIM_CUSTOMER c ON s.customer_id = c.customer_id
    WHERE c.account_manager = CURRENT_USER()
    OR CURRENT_ROLE() IN ('FLEET_MANAGER', 'OPERATIONS_MANAGER')
));

-- Apply the policy to location dimension
ALTER TABLE LOGISTICS_DW_PROD.MARTS.DIM_LOCATION 
ADD ROW ACCESS POLICY LOCATION_SERVICE_AREA_ACCESS ON (location_id);

-- Create RLS policy for financial data based on role
CREATE OR REPLACE ROW ACCESS POLICY LOGISTICS_DW_PROD.MARTS.FINANCIAL_DATA_ACCESS
AS (CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_ANALYST', 'FINANCE_TEAM', 'EXECUTIVE'));

-- Apply the policy to financial columns
ALTER TABLE LOGISTICS_DW_PROD.MARTS.TBL_FACT_SHIPMENTS 
ADD ROW ACCESS POLICY FINANCIAL_DATA_ACCESS ON (revenue_usd, total_cost_usd, profit_margin_pct);

-- Create RLS policy for sensitive customer data
CREATE OR REPLACE ROW ACCESS POLICY LOGISTICS_DW_PROD.MARTS.CUSTOMER_PII_ACCESS
AS (CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_STEWARD', 'CUSTOMER_SERVICE'));

-- Apply the policy to PII columns
ALTER TABLE LOGISTICS_DW_PROD.MARTS.TBL_DIM_CUSTOMER 
ADD ROW ACCESS POLICY CUSTOMER_PII_ACCESS ON (contact_email, contact_phone, billing_address);

-- Create RLS policy for maintenance data based on vehicle ownership
CREATE OR REPLACE ROW ACCESS POLICY LOGISTICS_DW_PROD.MARTS.MAINTENANCE_VEHICLE_ACCESS
AS (vehicle_id IN (
    SELECT vehicle_id 
    FROM LOGISTICS_DW_PROD.MARTS.TBL_DIM_VEHICLE 
    WHERE assigned_driver_id = CURRENT_USER()
    OR CURRENT_ROLE() IN ('MAINTENANCE_TEAM', 'FLEET_MANAGER')
));

-- Apply the policy to maintenance table
ALTER TABLE LOGISTICS_DW_PROD.MARTS.FACT_MAINTENANCE 
ADD ROW ACCESS POLICY MAINTENANCE_VEHICLE_ACCESS ON (vehicle_id);

-- Create RLS policy for route data based on service area
CREATE OR REPLACE ROW ACCESS POLICY LOGISTICS_DW_PROD.MARTS.ROUTE_SERVICE_AREA_ACCESS
AS (route_id IN (
    SELECT DISTINCT r.route_id 
    FROM LOGISTICS_DW_PROD.MARTS.DIM_ROUTE r
    INNER JOIN LOGISTICS_DW_PROD.MARTS.DIM_LOCATION l ON r.origin_location_id = l.location_id
    INNER JOIN LOGISTICS_DW_PROD.MARTS.TBL_FACT_SHIPMENTS s ON r.route_id = s.route_id
    INNER JOIN LOGISTICS_DW_PROD.MARTS.TBL_DIM_CUSTOMER c ON s.customer_id = c.customer_id
    WHERE c.account_manager = CURRENT_USER()
    OR CURRENT_ROLE() IN ('FLEET_MANAGER', 'OPERATIONS_MANAGER', 'ROUTE_PLANNER')
));

-- Apply the policy to route dimension
ALTER TABLE LOGISTICS_DW_PROD.MARTS.DIM_ROUTE 
ADD ROW ACCESS POLICY ROUTE_SERVICE_AREA_ACCESS ON (route_id);

-- Create RLS policy for analytics data based on business unit
CREATE OR REPLACE ROW ACCESS POLICY LOGISTICS_DW_PROD.ANALYTICS.BUSINESS_UNIT_ACCESS
AS (CURRENT_ROLE() IN ('DATA_ANALYST', 'BUSINESS_USER', 'EXECUTIVE'));

-- Apply the policy to analytics views
ALTER VIEW LOGISTICS_DW_PROD.ANALYTICS.PERFORMANCE_DASHBOARD 
ADD ROW ACCESS POLICY BUSINESS_UNIT_ACCESS;

-- Create RLS policy for ML features based on data scientist access
CREATE OR REPLACE ROW ACCESS POLICY LOGISTICS_DW_PROD.MARTS.ML_FEATURES_ACCESS
AS (CURRENT_ROLE() IN ('DATA_SCIENTIST', 'ML_ENGINEER', 'DATA_ENGINEER'));

-- Apply the policy to ML features table
ALTER TABLE LOGISTICS_DW_PROD.MARTS.ML_FEATURE_STORE 
ADD ROW ACCESS POLICY ML_FEATURES_ACCESS;

-- Create view to show RLS policy summary
CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.ANALYTICS.V_RLS_POLICY_SUMMARY AS
SELECT 
    policy_name,
    policy_kind,
    table_catalog as database_name,
    table_schema as schema_name,
    table_name,
    policy_status,
    created_on,
    comment
FROM information_schema.row_access_policies
WHERE table_catalog = 'LOGISTICS_DW_PROD'
ORDER BY table_schema, table_name, policy_name;

-- Grant access to RLS policy summary
GRANT SELECT ON LOGISTICS_DW_PROD.ANALYTICS.V_RLS_POLICY_SUMMARY TO ROLE DATA_STEWARD;
GRANT SELECT ON LOGISTICS_DW_PROD.ANALYTICS.V_RLS_POLICY_SUMMARY TO ROLE DATA_ENGINEER;

-- Create function to test RLS policies
CREATE OR REPLACE FUNCTION LOGISTICS_DW_PROD.ANALYTICS.TEST_RLS_POLICIES()
RETURNS TABLE (
    policy_name STRING,
    table_name STRING,
    test_result STRING,
    test_message STRING
)
LANGUAGE SQL
AS
$$
    SELECT 
        'SHIPMENTS_CUSTOMER_ACCESS' as policy_name,
        'TBL_FACT_SHIPMENTS' as table_name,
        CASE 
            WHEN COUNT(*) > 0 THEN 'PASS'
            ELSE 'FAIL'
        END as test_result,
        'RLS policy applied successfully' as test_message
    FROM LOGISTICS_DW_PROD.MARTS.TBL_FACT_SHIPMENTS
    WHERE customer_id IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'VEHICLE_FLEET_ACCESS' as policy_name,
        'TBL_FACT_VEHICLE_TELEMETRY' as table_name,
        CASE 
            WHEN COUNT(*) > 0 THEN 'PASS'
            ELSE 'FAIL'
        END as test_result,
        'RLS policy applied successfully' as test_message
    FROM LOGISTICS_DW_PROD.MARTS.TBL_FACT_VEHICLE_TELEMETRY
    WHERE vehicle_id IS NOT NULL
$$;
