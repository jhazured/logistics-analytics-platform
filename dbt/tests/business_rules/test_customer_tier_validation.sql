-- =====================================================
-- Consolidated Customer Tier Validation Test
-- Combines customer segmentation and tier consistency validation
-- =====================================================

WITH customer_segmentation AS (
    SELECT 
        customer_id,
        customer_name,
        customer_tier,
        credit_limit_usd,
        customer_since_date,
        DATEDIFF('day', customer_since_date, CURRENT_DATE()) as customer_tenure_days,
        CASE 
            WHEN customer_since_date IS NULL THEN 'UNKNOWN'
            WHEN credit_limit_usd >= 1000000 THEN 'PREMIUM'
            WHEN credit_limit_usd >= 100000 THEN 'STANDARD'
            WHEN credit_limit_usd >= 10000 THEN 'BASIC'
            ELSE 'BASIC'
        END as expected_tier
    FROM {{ ref('tbl_dim_customer') }}
    WHERE is_active = true
),

annual_metrics AS (
    SELECT 
        customer_id,
        SUM(revenue) as total_revenue_12m,
        COUNT(*) as total_shipments_12m
    FROM {{ ref('tbl_fact_shipments') }}
    WHERE shipment_date >= DATEADD('month', -12, CURRENT_DATE())
    GROUP BY customer_id
),

customer_validation AS (
    SELECT 
        cs.customer_id,
        cs.customer_name,
        cs.customer_tier,
        cs.expected_tier,
        cs.credit_limit_usd,
        cs.customer_tenure_days,
        COALESCE(am.total_revenue_12m, 0) as total_revenue_12m,
        COALESCE(am.total_shipments_12m, 0) as total_shipments_12m,
        
        -- Validation flags
        CASE 
            WHEN cs.customer_tier != cs.expected_tier THEN 'TIER_MISMATCH'
            WHEN cs.customer_tier = 'PREMIUM' 
                 AND COALESCE(am.total_revenue_12m, 0) < 100000 
                 AND cs.credit_limit_usd < 500000 THEN 'PREMIUM_UNDERQUALIFIED'
            WHEN cs.customer_tier = 'BASIC' 
                 AND am.total_revenue_12m > 500000 THEN 'BASIC_OVERQUALIFIED'
            WHEN cs.customer_tier = 'STANDARD' 
                 AND (am.total_revenue_12m > 1000000 OR cs.credit_limit_usd > 1000000) THEN 'STANDARD_OVERQUALIFIED'
            ELSE 'VALID'
        END as validation_status,
        
        -- Business rule violations
        CASE 
            WHEN cs.customer_tier != cs.expected_tier THEN 'Tier assignment does not match credit limit rules'
            WHEN cs.customer_tier = 'PREMIUM' 
                 AND COALESCE(am.total_revenue_12m, 0) < 100000 
                 AND cs.credit_limit_usd < 500000 THEN 'Premium customer lacks required revenue or credit limit'
            WHEN cs.customer_tier = 'BASIC' 
                 AND am.total_revenue_12m > 500000 THEN 'Basic customer has premium-level revenue'
            WHEN cs.customer_tier = 'STANDARD' 
                 AND (am.total_revenue_12m > 1000000 OR cs.credit_limit_usd > 1000000) THEN 'Standard customer qualifies for premium tier'
            ELSE 'Customer tier assignment is valid'
        END as validation_message
        
    FROM customer_segmentation cs
    LEFT JOIN annual_metrics am ON cs.customer_id = am.customer_id
)

SELECT 
    customer_id,
    customer_name,
    customer_tier,
    expected_tier,
    credit_limit_usd,
    customer_tenure_days,
    total_revenue_12m,
    total_shipments_12m,
    validation_status,
    validation_message
FROM customer_validation
WHERE validation_status != 'VALID'
