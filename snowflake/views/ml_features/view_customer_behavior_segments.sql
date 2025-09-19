- Customer clustering for personalized delivery predictions
SELECT 
    customer_id,
    CASE 
        WHEN avg_monthly_volume >= 100 THEN 'high_volume'
        WHEN avg_monthly_volume >= 20 THEN 'medium_volume'
        ELSE 'low_volume'
    END AS volume_segment,
    
    delivery_flexibility_score,  -- How flexible with timing
    premium_service_preference,  -- Willingness to pay for speed
    delivery_window_adherence    -- Historical pattern compliance