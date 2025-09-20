-- Automated Query Optimization Recommendations
-- This script provides intelligent query optimization suggestions based on performance analysis

-- Create query optimization recommendations table
CREATE OR REPLACE TABLE LOGISTICS_DW_PROD.PERFORMANCE.QUERY_OPTIMIZATION_RECOMMENDATIONS (
    RECOMMENDATION_ID VARCHAR(50) DEFAULT UUID_STRING(),
    QUERY_ID VARCHAR(100) NOT NULL,
    QUERY_TEXT TEXT NOT NULL,
    OPTIMIZATION_TYPE VARCHAR(100) NOT NULL,
    RECOMMENDATION_DESCRIPTION TEXT NOT NULL,
    POTENTIAL_TIME_SAVINGS_SECONDS FLOAT NOT NULL,
    POTENTIAL_COST_SAVINGS_USD FLOAT NOT NULL,
    CONFIDENCE_SCORE FLOAT NOT NULL,
    IMPLEMENTATION_EFFORT VARCHAR(20) NOT NULL,
    BUSINESS_IMPACT VARCHAR(20) NOT NULL,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create query pattern analysis function
CREATE OR REPLACE FUNCTION analyze_query_patterns(query_text TEXT)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
    CASE 
        -- SELECT * patterns
        WHEN query_text ILIKE '%SELECT *%' THEN
            OBJECT_CONSTRUCT(
                'pattern_type', 'SELECT_STAR',
                'optimization_type', 'COLUMN_SPECIFICATION',
                'description', 'Replace SELECT * with specific columns',
                'potential_savings', 0.3,
                'implementation_effort', 'LOW',
                'confidence_score', 0.9
            )
        
        -- Missing LIMIT on ORDER BY
        WHEN query_text ILIKE '%ORDER BY%' AND query_text NOT ILIKE '%LIMIT%' THEN
            OBJECT_CONSTRUCT(
                'pattern_type', 'MISSING_LIMIT',
                'optimization_type', 'RESULT_LIMITING',
                'description', 'Add LIMIT clause to ORDER BY queries',
                'potential_savings', 0.2,
                'implementation_effort', 'LOW',
                'confidence_score', 0.8
            )
        
        -- Large table scans without filters
        WHEN query_text ILIKE '%FROM%' AND query_text NOT ILIKE '%WHERE%' AND query_text NOT ILIKE '%JOIN%' THEN
            OBJECT_CONSTRUCT(
                'pattern_type', 'NO_FILTERS',
                'optimization_type', 'FILTER_OPTIMIZATION',
                'description', 'Add WHERE clause to reduce data scan',
                'potential_savings', 0.4,
                'implementation_effort', 'MEDIUM',
                'confidence_score', 0.7
            )
        
        -- Complex subqueries
        WHEN query_text ILIKE '%SELECT%SELECT%' AND query_text ILIKE '%FROM%' AND query_text ILIKE '%FROM%' THEN
            OBJECT_CONSTRUCT(
                'pattern_type', 'COMPLEX_SUBQUERY',
                'optimization_type', 'QUERY_REWRITE',
                'description', 'Consider rewriting as JOIN or CTE',
                'potential_savings', 0.5,
                'implementation_effort', 'HIGH',
                'confidence_score', 0.6
            )
        
        -- Missing indexes on JOIN columns
        WHEN query_text ILIKE '%JOIN%' AND query_text NOT ILIKE '%ON%' THEN
            OBJECT_CONSTRUCT(
                'pattern_type', 'MISSING_JOIN_CONDITION',
                'optimization_type', 'JOIN_OPTIMIZATION',
                'description', 'Add proper JOIN conditions',
                'potential_savings', 0.6,
                'implementation_effort', 'MEDIUM',
                'confidence_score', 0.8
            )
        
        -- Default
        ELSE
            OBJECT_CONSTRUCT(
                'pattern_type', 'NO_OPTIMIZATION',
                'optimization_type', 'NONE',
                'description', 'No obvious optimization patterns detected',
                'potential_savings', 0.0,
                'implementation_effort', 'NONE',
                'confidence_score', 0.0
            )
    END
$$;

-- Create query performance analysis view
CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.PERFORMANCE.VW_QUERY_PERFORMANCE_ANALYSIS AS
WITH slow_queries AS (
    SELECT 
        query_id,
        query_text,
        total_elapsed_time / 1000 as total_elapsed_time_seconds,
        compilation_time / 1000 as compilation_time_seconds,
        execution_time / 1000 as execution_time_seconds,
        bytes_scanned,
        rows_produced,
        warehouse_name,
        user_name,
        start_time,
        -- Calculate cost per query
        (credits_used_compute + credits_used_cloud_services) * 3.00 as query_cost_usd,
        -- Analyze query patterns
        analyze_query_patterns(query_text) as pattern_analysis
    FROM snowflake.account_usage.query_history
    WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    AND total_elapsed_time > 60000  -- Queries taking more than 1 minute
    AND query_type = 'SELECT'
    AND error_code IS NULL
),
optimization_opportunities AS (
    SELECT 
        *,
        -- Calculate potential savings
        total_elapsed_time_seconds * pattern_analysis:potential_savings::FLOAT as potential_time_savings,
        query_cost_usd * pattern_analysis:potential_savings::FLOAT as potential_cost_savings,
        -- Determine business impact
        CASE 
            WHEN total_elapsed_time_seconds > 300 THEN 'HIGH'
            WHEN total_elapsed_time_seconds > 120 THEN 'MEDIUM'
            ELSE 'LOW'
        END as business_impact
    FROM slow_queries
    WHERE pattern_analysis:optimization_type::VARCHAR != 'NONE'
)
SELECT 
    query_id,
    query_text,
    total_elapsed_time_seconds,
    query_cost_usd,
    warehouse_name,
    user_name,
    start_time,
    pattern_analysis:pattern_type::VARCHAR as pattern_type,
    pattern_analysis:optimization_type::VARCHAR as optimization_type,
    pattern_analysis:description::VARCHAR as recommendation_description,
    potential_time_savings,
    potential_cost_savings,
    pattern_analysis:confidence_score::FLOAT as confidence_score,
    pattern_analysis:implementation_effort::VARCHAR as implementation_effort,
    business_impact,
    -- Calculate ROI score
    CASE 
        WHEN potential_cost_savings > 0 THEN
            potential_cost_savings / 
            CASE 
                WHEN pattern_analysis:implementation_effort::VARCHAR = 'LOW' THEN 1
                WHEN pattern_analysis:implementation_effort::VARCHAR = 'MEDIUM' THEN 5
                WHEN pattern_analysis:implementation_effort::VARCHAR = 'HIGH' THEN 20
                ELSE 1
            END
        ELSE 0
    END as roi_score
FROM optimization_opportunities
ORDER BY potential_cost_savings DESC, potential_time_savings DESC;

-- Create automated query optimization recommendations procedure
CREATE OR REPLACE PROCEDURE LOGISTICS_DW_PROD.PERFORMANCE.GENERATE_QUERY_OPTIMIZATION_RECOMMENDATIONS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    recommendation_count INT;
    total_potential_savings FLOAT;
    result_message STRING;
BEGIN
    -- Clear existing recommendations
    DELETE FROM LOGISTICS_DW_PROD.PERFORMANCE.QUERY_OPTIMIZATION_RECOMMENDATIONS
    WHERE DATE(created_at) = CURRENT_DATE();
    
    -- Insert new recommendations
    INSERT INTO LOGISTICS_DW_PROD.PERFORMANCE.QUERY_OPTIMIZATION_RECOMMENDATIONS (
        query_id, query_text, optimization_type, recommendation_description,
        potential_time_savings_seconds, potential_cost_savings_usd, confidence_score,
        implementation_effort, business_impact
    )
    SELECT 
        query_id,
        LEFT(query_text, 1000) as query_text,  -- Truncate for storage
        optimization_type,
        recommendation_description,
        potential_time_savings,
        potential_cost_savings,
        confidence_score,
        implementation_effort,
        business_impact
    FROM LOGISTICS_DW_PROD.PERFORMANCE.VW_QUERY_PERFORMANCE_ANALYSIS
    WHERE potential_cost_savings > 1  -- Only recommendations with meaningful savings
    AND confidence_score > 0.6;  -- Only high-confidence recommendations
    
    -- Get summary statistics
    SELECT 
        COUNT(*),
        SUM(potential_cost_savings_usd)
    INTO recommendation_count, total_potential_savings
    FROM LOGISTICS_DW_PROD.PERFORMANCE.QUERY_OPTIMIZATION_RECOMMENDATIONS
    WHERE DATE(created_at) = CURRENT_DATE();
    
    result_message := 'Generated ' || recommendation_count || ' query optimization recommendations with potential savings of $' || 
                     ROUND(total_potential_savings, 2) || ' per day.';
    
    -- Send alert if significant savings identified
    IF total_potential_savings > 50 THEN
        INSERT INTO LOGISTICS_DW_PROD.MONITORING.ALERT_HISTORY (
            alert_type, alert_name, severity, alert_message, alert_data
        ) VALUES (
            'query_optimization', 'High Value Query Optimization', 'MEDIUM',
            result_message,
            OBJECT_CONSTRUCT('recommendation_count', recommendation_count, 'potential_savings', total_potential_savings)
        );
    END IF;
    
    RETURN result_message;
END;
$$;

-- Create query optimization dashboard
CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.PERFORMANCE.VW_QUERY_OPTIMIZATION_DASHBOARD AS
WITH query_performance_summary AS (
    SELECT 
        DATE_TRUNC('day', start_time) as query_date,
        COUNT(*) as total_queries,
        AVG(total_elapsed_time / 1000) as avg_query_time_seconds,
        SUM((credits_used_compute + credits_used_cloud_services) * 3.00) as total_query_cost_usd,
        COUNT(CASE WHEN total_elapsed_time > 60000 THEN 1 END) as slow_queries_count
    FROM snowflake.account_usage.query_history
    WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    AND query_type = 'SELECT'
    AND error_code IS NULL
    GROUP BY 1
),
optimization_summary AS (
    SELECT 
        COUNT(*) as total_recommendations,
        SUM(potential_cost_savings_usd) as total_potential_savings,
        AVG(confidence_score) as avg_confidence_score,
        COUNT(CASE WHEN implementation_effort = 'LOW' THEN 1 END) as low_effort_recommendations,
        COUNT(CASE WHEN business_impact = 'HIGH' THEN 1 END) as high_impact_recommendations,
        COUNT(CASE WHEN optimization_type = 'COLUMN_SPECIFICATION' THEN 1 END) as select_star_issues,
        COUNT(CASE WHEN optimization_type = 'FILTER_OPTIMIZATION' THEN 1 END) as filter_issues
    FROM LOGISTICS_DW_PROD.PERFORMANCE.QUERY_OPTIMIZATION_RECOMMENDATIONS
    WHERE DATE(created_at) = CURRENT_DATE()
)
SELECT 
    'Query Performance Overview' as metric_category,
    qps.total_queries as current_daily_queries,
    qps.avg_query_time_seconds as avg_query_time,
    qps.total_query_cost_usd as current_daily_cost,
    qps.slow_queries_count as slow_queries,
    os.total_potential_savings as potential_daily_savings,
    os.total_recommendations as active_recommendations,
    os.avg_confidence_score as avg_confidence,
    os.low_effort_recommendations as quick_wins,
    os.high_impact_recommendations as high_impact_items,
    os.select_star_issues as select_star_issues,
    os.filter_issues as filter_issues,
    -- Calculate performance score
    CASE 
        WHEN qps.avg_query_time_seconds < 10 THEN 100
        WHEN qps.avg_query_time_seconds < 30 THEN 80
        WHEN qps.avg_query_time_seconds < 60 THEN 60
        ELSE 40
    END as performance_score
FROM query_performance_summary qps
CROSS JOIN optimization_summary os
WHERE qps.query_date = CURRENT_DATE()

UNION ALL

SELECT 
    'Optimization Opportunities' as metric_category,
    COUNT(*) as current_daily_queries,
    AVG(potential_time_savings_seconds) as avg_query_time,
    SUM(potential_cost_savings_usd) as current_daily_cost,
    COUNT(CASE WHEN business_impact = 'HIGH' THEN 1 END) as slow_queries,
    SUM(potential_cost_savings_usd) as potential_daily_savings,
    COUNT(*) as active_recommendations,
    AVG(confidence_score) as avg_confidence,
    COUNT(CASE WHEN implementation_effort = 'LOW' THEN 1 END) as quick_wins,
    COUNT(CASE WHEN business_impact = 'HIGH' THEN 1 END) as high_impact_items,
    COUNT(CASE WHEN optimization_type = 'COLUMN_SPECIFICATION' THEN 1 END) as select_star_issues,
    COUNT(CASE WHEN optimization_type = 'FILTER_OPTIMIZATION' THEN 1 END) as filter_issues,
    -- Calculate optimization score
    AVG(roi_score) * 100 as performance_score
FROM LOGISTICS_DW_PROD.PERFORMANCE.VW_QUERY_PERFORMANCE_ANALYSIS
WHERE potential_cost_savings > 1;

-- Create automated query optimization task
CREATE OR REPLACE TASK LOGISTICS_DW_PROD.PERFORMANCE.TASK_QUERY_OPTIMIZATION_ANALYSIS
    WAREHOUSE = COMPUTE_WH_XS
    SCHEDULE = 'USING CRON 0 8 * * * UTC'  -- Daily at 8 AM UTC
    COMMENT = 'Generates daily query optimization recommendations'
AS
BEGIN
    CALL LOGISTICS_DW_PROD.PERFORMANCE.GENERATE_QUERY_OPTIMIZATION_RECOMMENDATIONS();
END;

-- Create query optimization alerts
CREATE OR REPLACE PROCEDURE LOGISTICS_DW_PROD.PERFORMANCE.CHECK_QUERY_OPTIMIZATION_ALERTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    high_cost_queries INT;
    high_savings_opportunities INT;
    alert_message STRING;
BEGIN
    -- Check for high-cost queries
    SELECT COUNT(*) INTO high_cost_queries
    FROM LOGISTICS_DW_PROD.PERFORMANCE.VW_QUERY_PERFORMANCE_ANALYSIS
    WHERE potential_cost_savings > 10
    AND business_impact = 'HIGH';
    
    -- Check for high savings opportunities
    SELECT COUNT(*) INTO high_savings_opportunities
    FROM LOGISTICS_DW_PROD.PERFORMANCE.QUERY_OPTIMIZATION_RECOMMENDATIONS
    WHERE DATE(created_at) = CURRENT_DATE()
    AND potential_cost_savings_usd > 5;
    
    IF high_cost_queries > 0 THEN
        alert_message := 'ALERT: ' || high_cost_queries || ' high-cost queries identified for optimization.';
        
        INSERT INTO LOGISTICS_DW_PROD.MONITORING.ALERT_HISTORY (
            alert_type, alert_name, severity, alert_message, alert_data
        ) VALUES (
            'query_optimization', 'High Cost Query Alert', 'MEDIUM',
            alert_message,
            OBJECT_CONSTRUCT('high_cost_queries', high_cost_queries)
        );
    END IF;
    
    IF high_savings_opportunities > 0 THEN
        alert_message := 'OPPORTUNITY: ' || high_savings_opportunities || ' high-value query optimization opportunities identified.';
        
        INSERT INTO LOGISTICS_DW_PROD.MONITORING.ALERT_HISTORY (
            alert_type, alert_name, severity, alert_message, alert_data
        ) VALUES (
            'query_optimization', 'High Value Query Optimization Alert', 'LOW',
            alert_message,
            OBJECT_CONSTRUCT('high_savings_opportunities', high_savings_opportunities)
        );
    END IF;
    
    RETURN alert_message;
END;
$$;

-- Resume the query optimization task
ALTER TASK LOGISTICS_DW_PROD.PERFORMANCE.TASK_QUERY_OPTIMIZATION_ANALYSIS RESUME;
