-- Test customer segmentation logic
-- Validates that customer tiers are assigned correctly based on business rules

WITH customer_segmentation AS (
    SELECT 
        customer_id,
        customer_tier,
        credit_limit_usd,
        customer_since_date,
        DATEDIFF('day', customer_since_date, CURRENT_DATE()) as customer_tenure_days,
        CASE 
            WHEN customer_since_date IS NULL THEN 'UNKNOWN'
            WHEN credit_limit_usd >= 1000000 AND DATEDIFF('day', customer_since_date, CURRENT_DATE()) >= 365 THEN 'PREMIUM'
            WHEN credit_limit_usd >= 1000000 AND DATEDIFF('day', customer_since_date, CURRENT_DATE()) < 365 THEN 'PREMIUM_NEW'
            WHEN credit_limit_usd >= 100000 THEN 'STANDARD'
            WHEN credit_limit_usd >= 10000 THEN 'BASIC'
            ELSE 'BASIC'
        END as expected_tier
    FROM {{ ref('dim_customer') }}
    WHERE is_active = true
)

SELECT 
    customer_id,
    customer_tier,
    expected_tier,
    credit_limit_usd,
    customer_tenure_days
FROM customer_segmentation
WHERE customer_tier != expected_tier
