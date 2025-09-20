-- dbt/models/marts/analytics/view_data_lineage.sql

{{ config(
    materialized='view',
    tags=['analytics', 'lineage', 'metadata']
) }}

WITH model_dependencies AS (
    SELECT 
        model_name,
        depends_on_models,
        materialization,
        tags,
        created_at,
        updated_at
    FROM {{ ref('dbt_models') }}
),

column_lineage AS (
    SELECT 
        table_name,
        column_name,
        data_type,
        is_nullable,
        column_description,
        source_table,
        source_column
    FROM {{ ref('dbt_columns') }}
),

data_flow AS (
    SELECT 
        'RAW' as layer,
        table_name,
        'Source System' as source_system,
        'Fivetran' as ingestion_method,
        CURRENT_TIMESTAMP() as last_updated
    FROM {{ ref('dbt_sources') }}
    
    UNION ALL
    
    SELECT 
        'STAGING' as layer,
        model_name as table_name,
        'dbt Staging' as source_system,
        'dbt Transform' as ingestion_method,
        updated_at as last_updated
    FROM model_dependencies
    WHERE materialization = 'view'
    
    UNION ALL
    
    SELECT 
        'MARTS' as layer,
        model_name as table_name,
        'dbt Marts' as source_system,
        'dbt Transform' as ingestion_method,
        updated_at as last_updated
    FROM model_dependencies
    WHERE materialization IN ('table', 'incremental')
)

SELECT 
    df.layer,
    df.table_name,
    df.source_system,
    df.ingestion_method,
    df.last_updated,
    md.materialization,
    md.tags,
    COUNT(cl.column_name) as column_count,
    STRING_AGG(cl.column_name, ', ') as columns
FROM data_flow df
LEFT JOIN model_dependencies md ON df.table_name = md.model_name
LEFT JOIN column_lineage cl ON df.table_name = cl.table_name
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY df.layer, df.table_name
