-- 13. Real-time Scoring
-- File: models/analytics/ml_features/view_real_time_scoring.sql
{{ config(
    materialized='view',
    tags=['ml', 'scoring', 'real_time']
) }}

WITH current_shipments AS (
    SELECT 
        fs.shipment_id,
        fs.route_id,
        fs.vehicle_id,
        fs.customer_id,
        fs.shipment_date,
        fs.planned_delivery_date,
        dr.total_distance_km,
        dr.complexity_score,
        dv.vehicle_type,
        dc.volume_segment,
        
        -- Real-time features
        EXTRACT(hour FROM CURRENT_TIMESTAMP()) AS current_hour,
        EXTRACT(dayofweek FROM CURRENT_DATE()) AS current_dow,
        {{ classify_haul_type('dr.total_distance_km') }} AS haul_type
        
    FROM {{ ref('fact_shipments') }} fs
    JOIN {{ ref('dim_route') }} dr ON fs.route_id = dr.route_id
    JOIN {{ ref('dim_vehicle') }} dv ON fs.vehicle_id = dv.vehicle_id
    JOIN {{ ref('dim_customer') }} dc ON fs.customer_id = dc.customer_id
    WHERE fs.shipment_date = CURRENT_DATE()
        AND fs.delivery_status = 'In Transit'
),

route_predictions AS (
    SELECT 
        cs.shipment_id,
        cs.route_id,
        
        -- Route optimization prediction (simplified ML scoring)
        rof.predicted_duration_minutes,
        rof.weather_delay_factor,
        rof.traffic_delay_factor,
        rof.route_recommendation,
        
        -- On-time probability based on historical performance
        LEAST(1.0, GREATEST(0.0,
            rof.historical_on_time_rate * 
            (2 - rof.weather_delay_factor) * 
            (2 - rof.traffic_delay_factor)
        )) AS predicted_on_time_probability,
        
        -- Delay risk classification
        CASE 
            WHEN rof.combined_delay_factor > 1.5 THEN 'high_risk'
            WHEN rof.combined_delay_factor > 1.2 THEN 'medium_risk'
            ELSE 'low_risk'
        END AS delay_risk_category
        
    FROM current_shipments cs
    LEFT JOIN {{ ref('view_route_optimization_features') }} rof 
        ON cs.route_id = rof.route_id 
        AND cs.haul_type = rof.haul_type
        AND rof.shipment_date = CURRENT_DATE()
),

customer_predictions AS (
    SELECT 
        cs.customer_id,
        
        -- Customer satisfaction prediction
        cbs.satisfaction_level,
        cbs.on_time_rate,
        cbs.activity_status,
        
        -- Churn risk scoring
        CASE 
            WHEN cbs.activity_status = 'churned' THEN 0.9
            WHEN cbs.activity_status = 'at_risk' THEN 0.6
            WHEN cbs.activity_status = 'moderate' THEN 0.3
            ELSE 0.1
        END AS churn_risk_score,
        
        -- Satisfaction prediction based on delivery performance
        LEAST(10, GREATEST(1,
            cbs.avg_customer_rating * 
            (1 + (cbs.on_time_rate - 0.8) * 2)
        )) AS predicted_satisfaction_score
        
    FROM current_shipments cs
    LEFT JOIN {{ ref('view_customer_behavior_segments') }} cbs 
        ON cs.customer_id = cbs.customer_id
),

vehicle_predictions AS (
    SELECT 
        cs.vehicle_id,
        
        -- Maintenance prediction
        pmf.breakdown_risk_score,
        pmf.will_degrade_30d,
        pmf.has_scheduled_maintenance_30d,
        
        -- Performance prediction
        CASE 
            WHEN pmf.breakdown_risk_score > 7 THEN 'high_maintenance_risk'
            WHEN pmf.breakdown_risk_score > 5 THEN 'medium_maintenance_risk'
            ELSE 'low_maintenance_risk'
        END AS maintenance_risk_category
        
    FROM current_shipments cs
    LEFT JOIN {{ ref('view_predictive_maintenance_features') }} pmf 
        ON cs.vehicle_id = pmf.vehicle_id 
        AND pmf.usage_date = CURRENT_DATE()
)

SELECT 
    cs.shipment_id,
    cs.route_id,
    cs.vehicle_id,
    cs.customer_id,
    cs.shipment_date,
    cs.planned_delivery_date,
    
    -- Route predictions
    rp.predicted_duration_minutes,
    rp.predicted_on_time_probability,
    rp.delay_risk_category,
    rp.route_recommendation,
    
    -- Customer predictions
    cp.churn_risk_score,
    cp.predicted_satisfaction_score,
    cp.satisfaction_level AS current_satisfaction_level,
    
    -- Vehicle predictions
    vp.breakdown_risk_score,
    vp.maintenance_risk_category,
    vp.will_degrade_30d AS vehicle_degradation_risk,
    
    -- Combined risk assessment
    GREATEST(
        CASE WHEN rp.delay_risk_category = 'high_risk' THEN 0.8
             WHEN rp.delay_risk_category = 'medium_risk' THEN 0.5
             ELSE 0.2 END,
        cp.churn_risk_score * 0.8,
        vp.breakdown_risk_score / 10.0 * 0.6
    ) AS overall_delivery_risk_score,
    
    -- Recommendations priority
    CASE 
        WHEN vp.breakdown_risk_score > 8 THEN 'immediate_vehicle_check'
        WHEN rp.delay_risk_category = 'high_risk' THEN 'route_optimization_needed'
        WHEN cp.churn_risk_score > 0.7 THEN 'customer_communication_priority'
        ELSE 'monitor_normal_operations'
    END AS primary_recommendation,
    
    -- Confidence scores
    ROUND(rp.predicted_on_time_probability * 100, 1) AS on_time_confidence_percent,
    ROUND((1 - cp.churn_risk_score) * 100, 1) AS customer_retention_confidence,
    
    CURRENT_TIMESTAMP() AS prediction_timestamp

FROM current_shipments cs
LEFT JOIN route_predictions rp ON cs.shipment_id = rp.shipment_id
LEFT JOIN customer_predictions cp ON cs.customer_id = cp.customer_id
LEFT JOIN vehicle_predictions vp ON cs.vehicle_id = vp.vehicle_id