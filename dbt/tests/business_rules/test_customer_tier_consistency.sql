-- Test customer tier assignments are consistent with business rules
SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_tier,
    c.credit_limit_usd,
    annual_revenue.total_revenue_12m,
    annual_shipments.total_shipments_12m
FROM {{ ref('dim_customer') }} c
LEFT JOIN (
    SELECT 
        customer_id,
        SUM(revenue_usd) as total_revenue_12m
    FROM {{ ref('fact_shipments') }}
    WHERE shipment_date >= DATEADD('month', -12, CURRENT_DATE())
    GROUP BY customer_id
) annual_revenue ON c.customer_id = annual_revenue.customer_id
LEFT JOIN (
    SELECT 
        customer_id,
        COUNT(*) as total_shipments_12m
    FROM {{ ref('fact_shipments') }}
    WHERE shipment_date >= DATEADD('month', -12, CURRENT_DATE())
    GROUP BY customer_id
) annual_shipments ON c.customer_id = annual_shipments.customer_id
WHERE 
    -- PREMIUM customers should have high revenue or credit limit
    (c.customer_tier = 'PREMIUM' 
     AND COALESCE(annual_revenue.total_revenue_12m, 0) < 100000 
     AND c.credit_limit_usd < 500000)
    OR
    -- BASIC customers should not have very high revenue
    (c.customer_tier = 'BASIC' 
     AND annual_revenue.total_revenue_12m > 500000)
    OR
    -- STANDARD customers should be in the middle range
    (c.customer_tier = 'STANDARD' 
     AND (annual_revenue.total_revenue_12m > 1000000 OR c.credit_limit_usd > 1000000))