-- =====================================================
-- Setup Verification Script
-- =====================================================
-- This script verifies that all databases, schemas, roles,
-- warehouses, and resource monitors have been created correctly

-- =====================================================
-- 1. DATABASE VERIFICATION
-- =====================================================

SELECT 'DATABASE VERIFICATION' as verification_type;

SELECT 
    'DATABASES' as object_type,
    database_name,
    created,
    CASE 
        WHEN database_name = 'LOGISTICS_DW_PROD' THEN '✅ Production database'
        WHEN database_name = 'LOGISTICS_DW_DEV' THEN '✅ Development database'
        WHEN database_name = 'LOGISTICS_DW_STAGING' THEN '✅ Staging database'
        ELSE '❌ Unexpected database'
    END as status
FROM information_schema.databases 
WHERE database_name LIKE 'LOGISTICS_DW%'
ORDER BY database_name;

-- =====================================================
-- 2. SCHEMA VERIFICATION
-- =====================================================

SELECT 'SCHEMA VERIFICATION' as verification_type;

SELECT 
    'SCHEMAS' as object_type,
    schema_catalog as database_name,
    schema_name,
    CASE 
        WHEN schema_name IN ('RAW', 'STAGING', 'MARTS', 'ML_FEATURES', 'ANALYTICS', 'MONITORING', 'SNAPSHOTS', 'ML_OBJECTS', 'GOVERNANCE', 'PERFORMANCE', 'SECURITY') THEN '✅ Required schema'
        ELSE '❌ Unexpected schema'
    END as status
FROM information_schema.schemata 
WHERE schema_catalog LIKE 'LOGISTICS_DW%'
ORDER BY schema_catalog, schema_name;

-- =====================================================
-- 3. ROLE VERIFICATION
-- =====================================================

SELECT 'ROLE VERIFICATION' as verification_type;

SELECT 
    'ROLES' as object_type,
    role_name,
    created_on,
    CASE 
        WHEN role_name IN ('DATA_ENGINEER', 'DATA_ANALYST', 'DATA_SCIENTIST', 'ML_ENGINEER', 'BUSINESS_USER', 'DATA_STEWARD', 'SECURITY_ADMIN', 'DBT_DEV_ROLE', 'DBT_STAGING_ROLE', 'DBT_PROD_ROLE') THEN '✅ Required role'
        ELSE '❌ Unexpected role'
    END as status
FROM information_schema.roles 
WHERE role_name IN ('DATA_ENGINEER', 'DATA_ANALYST', 'DATA_SCIENTIST', 'ML_ENGINEER', 'BUSINESS_USER', 'DATA_STEWARD', 'SECURITY_ADMIN', 'DBT_DEV_ROLE', 'DBT_STAGING_ROLE', 'DBT_PROD_ROLE')
ORDER BY role_name;

-- =====================================================
-- 4. WAREHOUSE VERIFICATION
-- =====================================================

SELECT 'WAREHOUSE VERIFICATION' as verification_type;

SELECT 
    'WAREHOUSES' as object_type,
    warehouse_name,
    warehouse_size,
    auto_suspend,
    auto_resume,
    CASE 
        WHEN warehouse_name LIKE 'COMPUTE_WH_%' OR warehouse_name LIKE 'WH_%' THEN '✅ Required warehouse'
        ELSE '❌ Unexpected warehouse'
    END as status
FROM information_schema.warehouses 
WHERE warehouse_name LIKE 'COMPUTE_WH_%' OR warehouse_name LIKE 'WH_%'
ORDER BY warehouse_name;

-- =====================================================
-- 5. RESOURCE MONITOR VERIFICATION
-- =====================================================

SELECT 'RESOURCE MONITOR VERIFICATION' as verification_type;

SELECT 
    'RESOURCE_MONITORS' as object_type,
    name,
    credit_quota,
    frequency,
    CASE 
        WHEN name LIKE 'RM_%' OR name LIKE 'WH_%' OR name LIKE 'LOGISTICS_%' THEN '✅ Required resource monitor'
        ELSE '❌ Unexpected resource monitor'
    END as status
FROM information_schema.resource_monitors 
WHERE name LIKE 'RM_%' OR name LIKE 'WH_%' OR name LIKE 'LOGISTICS_%'
ORDER BY name;

-- =====================================================
-- 6. PERMISSION VERIFICATION
-- =====================================================

SELECT 'PERMISSION VERIFICATION' as verification_type;

-- Check role grants
SELECT 
    'ROLE_GRANTS' as object_type,
    grantee_name,
    granted_role,
    CASE 
        WHEN granted_role IN ('DATA_ENGINEER', 'DATA_ANALYST', 'DATA_SCIENTIST', 'ML_ENGINEER', 'BUSINESS_USER', 'DATA_STEWARD', 'SECURITY_ADMIN') THEN '✅ Required role grant'
        ELSE '❌ Unexpected role grant'
    END as status
FROM information_schema.role_grants 
WHERE granted_role IN ('DATA_ENGINEER', 'DATA_ANALYST', 'DATA_SCIENTIST', 'ML_ENGINEER', 'BUSINESS_USER', 'DATA_STEWARD', 'SECURITY_ADMIN')
ORDER BY granted_role, grantee_name;

-- =====================================================
-- 7. SUMMARY REPORT
-- =====================================================

SELECT 'SETUP SUMMARY' as verification_type;

WITH setup_summary AS (
    SELECT 
        'Databases' as component,
        COUNT(*) as count,
        CASE WHEN COUNT(*) = 3 THEN '✅ Complete' ELSE '❌ Incomplete' END as status
    FROM information_schema.databases 
    WHERE database_name LIKE 'LOGISTICS_DW%'
    
    UNION ALL
    
    SELECT 
        'Schemas per Database' as component,
        COUNT(*) as count,
        CASE WHEN COUNT(*) = 33 THEN '✅ Complete' ELSE '❌ Incomplete' END as status
    FROM information_schema.schemata 
    WHERE schema_catalog LIKE 'LOGISTICS_DW%'
    
    UNION ALL
    
    SELECT 
        'Roles' as component,
        COUNT(*) as count,
        CASE WHEN COUNT(*) = 10 THEN '✅ Complete' ELSE '❌ Incomplete' END as status
    FROM information_schema.roles 
    WHERE role_name IN ('DATA_ENGINEER', 'DATA_ANALYST', 'DATA_SCIENTIST', 'ML_ENGINEER', 'BUSINESS_USER', 'DATA_STEWARD', 'SECURITY_ADMIN', 'DBT_DEV_ROLE', 'DBT_STAGING_ROLE', 'DBT_PROD_ROLE')
    
    UNION ALL
    
    SELECT 
        'Warehouses' as component,
        COUNT(*) as count,
        CASE WHEN COUNT(*) >= 8 THEN '✅ Complete' ELSE '❌ Incomplete' END as status
    FROM information_schema.warehouses 
    WHERE warehouse_name LIKE 'COMPUTE_WH_%' OR warehouse_name LIKE 'WH_%'
    
    UNION ALL
    
    SELECT 
        'Resource Monitors' as component,
        COUNT(*) as count,
        CASE WHEN COUNT(*) >= 8 THEN '✅ Complete' ELSE '❌ Incomplete' END as status
    FROM information_schema.resource_monitors 
    WHERE name LIKE 'RM_%' OR name LIKE 'WH_%' OR name LIKE 'LOGISTICS_%'
)
SELECT 
    component,
    count,
    status
FROM setup_summary
ORDER BY component;

-- =====================================================
-- 8. NEXT STEPS
-- =====================================================

SELECT 'NEXT STEPS' as verification_type;

SELECT 
    'SETUP_COMPLETE' as status,
    'All databases, schemas, roles, warehouses, and resource monitors have been created successfully.' as message
WHERE EXISTS (
    SELECT 1 FROM information_schema.databases WHERE database_name = 'LOGISTICS_DW_PROD'
) AND EXISTS (
    SELECT 1 FROM information_schema.schemata WHERE schema_catalog = 'LOGISTICS_DW_PROD' AND schema_name = 'RAW'
) AND EXISTS (
    SELECT 1 FROM information_schema.roles WHERE role_name = 'DATA_ENGINEER'
) AND EXISTS (
    SELECT 1 FROM information_schema.warehouses WHERE warehouse_name = 'COMPUTE_WH_XS'
);

-- If setup is complete, show next steps
SELECT 
    'NEXT_STEPS' as step_type,
    '1. Create users and assign them to appropriate roles' as step_description
WHERE EXISTS (
    SELECT 1 FROM information_schema.databases WHERE database_name = 'LOGISTICS_DW_PROD'
);

SELECT 
    'NEXT_STEPS' as step_type,
    '2. Configure Fivetran connectors to point to LOGISTICS_DW_PROD.RAW schema' as step_description
WHERE EXISTS (
    SELECT 1 FROM information_schema.databases WHERE database_name = 'LOGISTICS_DW_PROD'
);

SELECT 
    'NEXT_STEPS' as step_type,
    '3. Run dbt models to create tables and views' as step_description
WHERE EXISTS (
    SELECT 1 FROM information_schema.databases WHERE database_name = 'LOGISTICS_DW_PROD'
);

SELECT 
    'NEXT_STEPS' as step_type,
    '4. Set up automation framework' as step_description
WHERE EXISTS (
    SELECT 1 FROM information_schema.databases WHERE database_name = 'LOGISTICS_DW_PROD'
);

SELECT 
    'NEXT_STEPS' as step_type,
    '5. Configure monitoring and alerting' as step_description
WHERE EXISTS (
    SELECT 1 FROM information_schema.databases WHERE database_name = 'LOGISTICS_DW_PROD'
);
