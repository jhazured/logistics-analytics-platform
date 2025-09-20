-- Predictive Cost Optimization with Automated Recommendations
-- This script implements advanced FinOps capabilities with ML-driven cost optimization

-- Create predictive cost optimization table
CREATE OR REPLACE TABLE LOGISTICS_DW_PROD.PERFORMANCE.PREDICTIVE_COST_OPTIMIZATION (
    OPTIMIZATION_ID VARCHAR(50) DEFAULT UUID_STRING(),
    WAREHOUSE_NAME VARCHAR(100) NOT NULL,
    OPTIMIZATION_DATE DATE NOT NULL,
    CURRENT_COST_USD FLOAT NOT NULL,
    PREDICTED_COST_USD FLOAT NOT NULL,
    OPTIMIZATION_RECOMMENDATION VARCHAR(100) NOT NULL,
    POTENTIAL_SAVINGS_USD FLOAT NOT NULL,
    CONFIDENCE_SCORE FLOAT NOT NULL,
    IMPLEMENTATION_EFFORT VARCHAR(20) NOT NULL,
    BUSINESS_IMPACT VARCHAR(20) NOT NULL,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create cost trend analysis function
CREATE OR REPLACE FUNCTION calculate_cost_trend(
    warehouse_name VARCHAR,
    days_back INT
)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    SELECT 
        CASE 
            WHEN COUNT(*) < 2 THEN 0
            ELSE (
                SELECT AVG(daily_cost) 
                FROM (
                    SELECT 
                        DATE_TRUNC('day', start_time) as cost_date,
                        SUM(credits_used_compute + credits_used_cloud_services) * 3.00 as daily_cost
                    FROM snowflake.account_usage.warehouse_metering_history
                    WHERE warehouse_name = warehouse_name
                    AND start_time >= DATEADD('day', -days_back, CURRENT_TIMESTAMP())
                    GROUP BY 1
                    ORDER BY 1 DESC
                    LIMIT 7
                )
            ) - (
                SELECT AVG(daily_cost)
                FROM (
                    SELECT 
                        DATE_TRUNC('day', start_time) as cost_date,
                        SUM(credits_used_compute + credits_used_cloud_services) * 3.00 as daily_cost
                    FROM snowflake.account_usage.warehouse_metering_history
                    WHERE warehouse_name = warehouse_name
                    AND start_time BETWEEN DATEADD('day', -days_back*2, CURRENT_TIMESTAMP()) 
                                      AND DATEADD('day', -days_back, CURRENT_TIMESTAMP())
                    GROUP BY 1
                    ORDER BY 1 DESC
                    LIMIT 7
                )
            )
        END
$$;

-- Create warehouse utilization analysis function
CREATE OR REPLACE FUNCTION calculate_warehouse_utilization(
    warehouse_name VARCHAR,
    days_back INT
)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    SELECT 
        AVG(
            CASE 
                WHEN queued_provisioning_time > 0 THEN 1.0
                WHEN queued_repair_time > 0 THEN 1.0
                WHEN queued_overload_time > 0 THEN 1.0
                ELSE 0.0
            END
        ) as utilization_score
    FROM snowflake.account_usage.query_history
    WHERE warehouse_name = warehouse_name
    AND start_time >= DATEADD('day', -days_back, CURRENT_TIMESTAMP())
$$;

-- Create cost optimization recommendations function
CREATE OR REPLACE FUNCTION generate_cost_optimization_recommendation(
    warehouse_name VARCHAR,
    current_cost FLOAT,
    predicted_cost FLOAT,
    utilization_score FLOAT,
    cost_trend FLOAT
)
RETURNS OBJECT
LANGUAGE SQL
AS
$$
    CASE 
        -- High utilization, high cost trend - Scale up
        WHEN utilization_score > 0.8 AND cost_trend > 0.2 THEN
            OBJECT_CONSTRUCT(
                'recommendation', 'SCALE_UP',
                'reason', 'High utilization with increasing costs',
                'potential_savings', 0,
                'confidence_score', 0.9,
                'implementation_effort', 'LOW',
                'business_impact', 'HIGH'
            )
        
        -- Low utilization, high cost - Scale down
        WHEN utilization_score < 0.3 AND current_cost > 100 THEN
            OBJECT_CONSTRUCT(
                'recommendation', 'SCALE_DOWN',
                'reason', 'Low utilization with high costs',
                'potential_savings', current_cost * 0.3,
                'confidence_score', 0.8,
                'implementation_effort', 'LOW',
                'business_impact', 'MEDIUM'
            )
        
        -- High cost trend, medium utilization - Optimize queries
        WHEN cost_trend > 0.3 AND utilization_score BETWEEN 0.3 AND 0.7 THEN
            OBJECT_CONSTRUCT(
                'recommendation', 'QUERY_OPTIMIZATION',
                'reason', 'Increasing costs with moderate utilization',
                'potential_savings', current_cost * 0.2,
                'confidence_score', 0.7,
                'implementation_effort', 'MEDIUM',
                'business_impact', 'HIGH'
            )
        
        -- High predicted cost - Implement auto-scaling
        WHEN predicted_cost > current_cost * 1.5 THEN
            OBJECT_CONSTRUCT(
                'recommendation', 'AUTO_SCALING',
                'reason', 'Predicted cost increase requires auto-scaling',
                'potential_savings', (predicted_cost - current_cost) * 0.4,
                'confidence_score', 0.8,
                'implementation_effort', 'HIGH',
                'business_impact', 'HIGH'
            )
        
        -- Default - Monitor
        ELSE
            OBJECT_CONSTRUCT(
                'recommendation', 'MONITOR',
                'reason', 'Costs within acceptable range',
                'potential_savings', 0,
                'confidence_score', 0.6,
                'implementation_effort', 'LOW',
                'business_impact', 'LOW'
            )
    END
$$;

-- Create predictive cost analysis view
CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.PERFORMANCE.VW_PREDICTIVE_COST_ANALYSIS AS
WITH cost_trends AS (
    SELECT 
        warehouse_name,
        DATE_TRUNC('day', start_time) as cost_date,
        SUM(credits_used_compute + credits_used_cloud_services) * 3.00 as daily_cost_usd,
        COUNT(*) as query_count,
        AVG(total_elapsed_time / 1000) as avg_query_time_seconds,
        SUM(bytes_scanned) / (1024*1024*1024) as total_gb_scanned
    FROM snowflake.account_usage.warehouse_metering_history wmh
    LEFT JOIN snowflake.account_usage.query_history qh 
        ON wmh.warehouse_name = qh.warehouse_name 
        AND DATE_TRUNC('day', wmh.start_time) = DATE_TRUNC('day', qh.start_time)
    WHERE wmh.start_time >= DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
),
cost_predictions AS (
    SELECT 
        warehouse_name,
        AVG(daily_cost_usd) as avg_daily_cost,
        STDDEV(daily_cost_usd) as cost_volatility,
        -- Predict next 7 days using trend analysis
        AVG(daily_cost_usd) + (STDDEV(daily_cost_usd) * 0.5) as predicted_7d_cost,
        -- Predict next 30 days
        AVG(daily_cost_usd) * 30 + (STDDEV(daily_cost_usd) * 30 * 0.3) as predicted_30d_cost,
        AVG(query_count) as avg_daily_queries,
        AVG(avg_query_time_seconds) as avg_query_time,
        AVG(total_gb_scanned) as avg_daily_gb_scanned
    FROM cost_trends
    GROUP BY 1
),
optimization_analysis AS (
    SELECT 
        cp.*,
        calculate_cost_trend(cp.warehouse_name, 7) as cost_trend_7d,
        calculate_cost_trend(cp.warehouse_name, 30) as cost_trend_30d,
        calculate_warehouse_utilization(cp.warehouse_name, 7) as utilization_score,
        -- Calculate cost efficiency metrics
        cp.avg_daily_cost / NULLIF(cp.avg_daily_queries, 0) as cost_per_query,
        cp.avg_daily_cost / NULLIF(cp.avg_daily_gb_scanned, 0) as cost_per_gb,
        -- Generate optimization recommendations
        generate_cost_optimization_recommendation(
            cp.warehouse_name,
            cp.avg_daily_cost,
            cp.predicted_7d_cost,
            calculate_warehouse_utilization(cp.warehouse_name, 7),
            calculate_cost_trend(cp.warehouse_name, 7)
        ) as optimization_recommendation
    FROM cost_predictions cp
)
SELECT 
    warehouse_name,
    avg_daily_cost as current_daily_cost_usd,
    predicted_7d_cost,
    predicted_30d_cost,
    cost_trend_7d,
    cost_trend_30d,
    utilization_score,
    cost_per_query,
    cost_per_gb,
    optimization_recommendation:recommendation::VARCHAR as recommendation,
    optimization_recommendation:reason::VARCHAR as reason,
    optimization_recommendation:potential_savings::FLOAT as potential_savings_usd,
    optimization_recommendation:confidence_score::FLOAT as confidence_score,
    optimization_recommendation:implementation_effort::VARCHAR as implementation_effort,
    optimization_recommendation:business_impact::VARCHAR as business_impact,
    -- Calculate ROI
    CASE 
        WHEN optimization_recommendation:potential_savings::FLOAT > 0 THEN
            optimization_recommendation:potential_savings::FLOAT / 
            CASE 
                WHEN optimization_recommendation:implementation_effort::VARCHAR = 'LOW' THEN 1
                WHEN optimization_recommendation:implementation_effort::VARCHAR = 'MEDIUM' THEN 5
                WHEN optimization_recommendation:implementation_effort::VARCHAR = 'HIGH' THEN 20
                ELSE 1
            END
        ELSE 0
    END as roi_score
FROM optimization_analysis
ORDER BY potential_savings_usd DESC;

-- Create automated cost optimization recommendations
CREATE OR REPLACE PROCEDURE LOGISTICS_DW_PROD.PERFORMANCE.SP_generate_cost_optimization_recommendations()
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
    DELETE FROM LOGISTICS_DW_PROD.PERFORMANCE.PREDICTIVE_COST_OPTIMIZATION
    WHERE optimization_date = CURRENT_DATE();
    
    -- Insert new recommendations
    INSERT INTO LOGISTICS_DW_PROD.PERFORMANCE.PREDICTIVE_COST_OPTIMIZATION (
        warehouse_name, optimization_date, current_cost_usd, predicted_cost_usd,
        optimization_recommendation, potential_savings_usd, confidence_score,
        implementation_effort, business_impact
    )
    SELECT 
        warehouse_name,
        CURRENT_DATE(),
        current_daily_cost_usd,
        predicted_7d_cost,
        recommendation,
        potential_savings_usd,
        confidence_score,
        implementation_effort,
        business_impact
    FROM LOGISTICS_DW_PROD.PERFORMANCE.VW_PREDICTIVE_COST_ANALYSIS
    WHERE recommendation != 'MONITOR'
    AND potential_savings_usd > 0;
    
    -- Get summary statistics
    SELECT 
        COUNT(*),
        SUM(potential_savings_usd)
    INTO recommendation_count, total_potential_savings
    FROM LOGISTICS_DW_PROD.PERFORMANCE.PREDICTIVE_COST_OPTIMIZATION
    WHERE optimization_date = CURRENT_DATE();
    
    result_message := 'Generated ' || recommendation_count || ' cost optimization recommendations with potential savings of $' || 
                     ROUND(total_potential_savings, 2) || ' per day.';
    
    -- Send alert if significant savings identified
    IF total_potential_savings > 100 THEN
        INSERT INTO LOGISTICS_DW_PROD.MONITORING.ALERT_HISTORY (
            alert_type, alert_name, severity, alert_message, alert_data
        ) VALUES (
            'cost_optimization', 'High Value Cost Optimization', 'MEDIUM',
            result_message,
            OBJECT_CONSTRUCT('recommendation_count', recommendation_count, 'potential_savings', total_potential_savings)
        );
    END IF;
    
    RETURN result_message;
END;
$$;

-- Create cost optimization dashboard
CREATE OR REPLACE VIEW LOGISTICS_DW_PROD.PERFORMANCE.VW_COST_OPTIMIZATION_DASHBOARD AS
WITH daily_cost_summary AS (
    SELECT 
        DATE_TRUNC('day', start_time) as cost_date,
        SUM(credits_used_compute + credits_used_cloud_services) * 3.00 as total_daily_cost,
        COUNT(DISTINCT warehouse_name) as active_warehouses,
        AVG(credits_used_compute + credits_used_cloud_services) * 3.00 as avg_warehouse_cost
    FROM snowflake.account_usage.warehouse_metering_history
    WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    GROUP BY 1
),
optimization_summary AS (
    SELECT 
        COUNT(*) as total_recommendations,
        SUM(potential_savings_usd) as total_potential_savings,
        AVG(confidence_score) as avg_confidence_score,
        COUNT(CASE WHEN implementation_effort = 'LOW' THEN 1 END) as low_effort_recommendations,
        COUNT(CASE WHEN business_impact = 'HIGH' THEN 1 END) as high_impact_recommendations
    FROM LOGISTICS_DW_PROD.PERFORMANCE.PREDICTIVE_COST_OPTIMIZATION
    WHERE optimization_date = CURRENT_DATE()
)
SELECT 
    'Cost Optimization Overview' as metric_category,
    dcs.total_daily_cost as current_daily_cost,
    os.total_potential_savings as potential_daily_savings,
    os.total_recommendations as active_recommendations,
    os.avg_confidence_score as avg_confidence,
    os.low_effort_recommendations as quick_wins,
    os.high_impact_recommendations as high_impact_items,
    -- Calculate savings percentage
    CASE 
        WHEN dcs.total_daily_cost > 0 THEN 
            (os.total_potential_savings / dcs.total_daily_cost) * 100
        ELSE 0
    END as potential_savings_percentage
FROM daily_cost_summary dcs
CROSS JOIN optimization_summary os
WHERE dcs.cost_date = CURRENT_DATE()

UNION ALL

SELECT 
    'Warehouse Performance' as metric_category,
    AVG(current_daily_cost_usd) as current_daily_cost,
    SUM(potential_savings_usd) as potential_daily_savings,
    COUNT(*) as active_recommendations,
    AVG(confidence_score) as avg_confidence,
    COUNT(CASE WHEN implementation_effort = 'LOW' THEN 1 END) as quick_wins,
    COUNT(CASE WHEN business_impact = 'HIGH' THEN 1 END) as high_impact_items,
    -- Calculate efficiency score
    AVG(
        CASE 
            WHEN cost_per_query < 0.1 THEN 1.0
            WHEN cost_per_query < 0.5 THEN 0.8
            WHEN cost_per_query < 1.0 THEN 0.6
            ELSE 0.4
        END
    ) * 100 as potential_savings_percentage
FROM LOGISTICS_DW_PROD.PERFORMANCE.VW_PREDICTIVE_COST_ANALYSIS
WHERE recommendation != 'MONITOR';

-- Create automated cost optimization task
CREATE OR REPLACE TASK LOGISTICS_DW_PROD.PERFORMANCE.TASK_COST_OPTIMIZATION_ANALYSIS
    WAREHOUSE = COMPUTE_WH_XS
    SCHEDULE = 'USING CRON 0 6 * * * UTC'  -- Daily at 6 AM UTC
    COMMENT = 'Generates daily cost optimization recommendations'
AS
BEGIN
    CALL LOGISTICS_DW_PROD.PERFORMANCE.SP_generate_cost_optimization_recommendations();
END;

-- Create cost optimization alerts
CREATE OR REPLACE PROCEDURE LOGISTICS_DW_PROD.PERFORMANCE.SP_check_cost_optimization_alerts()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    high_cost_warehouses INT;
    high_savings_opportunities INT;
    alert_message STRING;
BEGIN
    -- Check for high-cost warehouses
    SELECT COUNT(*) INTO high_cost_warehouses
    FROM LOGISTICS_DW_PROD.PERFORMANCE.VW_PREDICTIVE_COST_ANALYSIS
    WHERE current_daily_cost_usd > 500
    AND recommendation = 'SCALE_DOWN';
    
    -- Check for high savings opportunities
    SELECT COUNT(*) INTO high_savings_opportunities
    FROM LOGISTICS_DW_PROD.PERFORMANCE.PREDICTIVE_COST_OPTIMIZATION
    WHERE optimization_date = CURRENT_DATE()
    AND potential_savings_usd > 100;
    
    IF high_cost_warehouses > 0 THEN
        alert_message := 'ALERT: ' || high_cost_warehouses || ' warehouses have high costs and can be optimized.';
        
        INSERT INTO LOGISTICS_DW_PROD.MONITORING.ALERT_HISTORY (
            alert_type, alert_name, severity, alert_message, alert_data
        ) VALUES (
            'cost_optimization', 'High Cost Warehouse Alert', 'MEDIUM',
            alert_message,
            OBJECT_CONSTRUCT('high_cost_warehouses', high_cost_warehouses)
        );
    END IF;
    
    IF high_savings_opportunities > 0 THEN
        alert_message := 'OPPORTUNITY: ' || high_savings_opportunities || ' high-value cost optimization opportunities identified.';
        
        INSERT INTO LOGISTICS_DW_PROD.MONITORING.ALERT_HISTORY (
            alert_type, alert_name, severity, alert_message, alert_data
        ) VALUES (
            'cost_optimization', 'High Value Optimization Alert', 'LOW',
            alert_message,
            OBJECT_CONSTRUCT('high_savings_opportunities', high_savings_opportunities)
        );
    END IF;
    
    RETURN alert_message;
END;
$$;

-- Resume the cost optimization task
ALTER TASK LOGISTICS_DW_PROD.PERFORMANCE.TASK_COST_OPTIMIZATION_ANALYSIS RESUME;
