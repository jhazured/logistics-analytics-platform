-- Procedure to automatically resize warehouses based on usage
CREATE OR REPLACE PROCEDURE SP_optimize_warehouse_sizing()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // Get warehouse usage metrics
    var usage_query = `
        SELECT warehouse_name, 
               AVG(credits_used) as avg_credits,
               COUNT(*) as query_count,
               AVG(execution_time) as avg_execution_time
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
        WHERE start_time >= CURRENT_DATE() - 7
        GROUP BY warehouse_name
    `;
    
    var usage_stmt = snowflake.createStatement({sqlText: usage_query});
    var usage_result = usage_stmt.execute();
    
    var recommendations = [];
    
    while (usage_result.next()) {
        var warehouse = usage_result.getColumnValue(1);
        var avg_credits = usage_result.getColumnValue(2);
        var query_count = usage_result.getColumnValue(3);
        var avg_time = usage_result.getColumnValue(4);
        
        // Simple optimization logic
        if (avg_credits < 0.1 && query_count < 100) {
            recommendations.push(warehouse + ": Consider downsizing - low usage");
        } else if (avg_time > 300000 && avg_credits > 1.0) {
            recommendations.push(warehouse + ": Consider upsizing - long execution times");
        }
    }
    
    return recommendations.join('; ');
$$;