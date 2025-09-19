-- Test for unusual demand patterns that might indicate data quality issues
WITH monthly_shipments AS (
    SELECT 
        EXTRACT(YEAR FROM shipment_date) as year,
        EXTRACT(MONTH FROM shipment_date) as month,
        COUNT(*) as shipment_count,
        AVG(COUNT(*)) OVER (PARTITION BY EXTRACT(MONTH FROM shipment_date)) as avg_for_month
    FROM {{ ref('fact_shipments') }}
    WHERE shipment_date >= DATEADD('year', -2, CURRENT_DATE())
    GROUP BY EXTRACT(YEAR FROM shipment_date), EXTRACT(MONTH FROM shipment_date)
)
SELECT 
    year,
    month,
    shipment_count,
    avg_for_month,
    ABS(shipment_count - avg_for_month) / avg_for_month * 100 as deviation_pct
FROM monthly_shipments
WHERE 
    -- Flag months with extreme deviations (more than 50% from average)
    ABS(shipment_count - avg_for_month) / avg_for_month > 0.5
    AND shipment_count > 10  -- Ignore very low volume months
