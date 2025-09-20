-- Test KPI calculation logic
-- Validates that key performance indicators are calculated correctly

WITH kpi_validation AS (
    SELECT 
        shipment_id,
        actual_delivery_time_hours,
        estimated_delivery_time_hours,
        on_time_delivery_flag,
        revenue_usd,
        total_cost_usd,
        profit_margin_pct,
        
        -- Calculate expected values
        CASE 
            WHEN actual_delivery_time_hours <= estimated_delivery_time_hours * 1.1 THEN 1 
            ELSE 0 
        END as expected_on_time_flag,
        
        CASE 
            WHEN total_cost_usd > 0 THEN ((revenue_usd - total_cost_usd) / total_cost_usd) * 100
            ELSE 0 
        END as expected_profit_margin,
        
        -- Validate reasonable ranges
        CASE 
            WHEN actual_delivery_time_hours < 0 OR actual_delivery_time_hours > 168 THEN 1 
            ELSE 0 
        END as invalid_delivery_time,
        
        CASE 
            WHEN revenue_usd < 0 OR revenue_usd > 1000000 THEN 1 
            ELSE 0 
        END as invalid_revenue,
        
        CASE 
            WHEN total_cost_usd < 0 OR total_cost_usd > 500000 THEN 1 
            ELSE 0 
        END as invalid_cost
        
    FROM {{ ref('fact_shipments') }}
    WHERE shipment_date >= DATEADD('day', -30, CURRENT_DATE())
)

SELECT 
    shipment_id,
    on_time_delivery_flag,
    expected_on_time_flag,
    profit_margin_pct,
    expected_profit_margin,
    invalid_delivery_time,
    invalid_revenue,
    invalid_cost
FROM kpi_validation
WHERE 
    on_time_delivery_flag != expected_on_time_flag
    OR ABS(profit_margin_pct - expected_profit_margin) > 0.01
    OR invalid_delivery_time = 1
    OR invalid_revenue = 1
    OR invalid_cost = 1
