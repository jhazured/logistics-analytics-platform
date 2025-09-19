-- Real-time vehicle tracking and alerts
CREATE OR REPLACE TABLE real_time_vehicle_alerts (
    alert_id VARCHAR(50) DEFAULT UUID_STRING(),
    vehicle_id VARCHAR(20),
    alert_type VARCHAR(50),
    severity VARCHAR(20),
    message TEXT,
    alert_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    resolved_at TIMESTAMP_NTZ,
    resolved_by VARCHAR(100)
);

CREATE OR REPLACE TASK vehicle_monitoring_alerts
WAREHOUSE = COMPUTE_WH_XS
SCHEDULE = '30 SECONDS'
COMMENT = 'Monitor vehicle telemetry for real-time alerts'
AS
INSERT INTO real_time_vehicle_alerts (vehicle_id, alert_type, severity, message)
WITH telemetry_updates AS (
    SELECT 
        vehicle_id,
        engine_temperature,
        fuel_level,
        speed_mph,
        location_lat,
        location_lon,
        telemetry_timestamp
    FROM vehicle_telemetry_stream
    WHERE METADATA$ACTION = 'INSERT'
),
alerts AS (
    -- Engine temperature alerts
    SELECT 
        vehicle_id,
        'ENGINE_OVERHEATING' as alert_type,
        CASE WHEN engine_temperature > 250 THEN 'CRITICAL' ELSE 'WARNING' END as severity,
        'Engine temperature: ' || engine_temperature || '°F' as message
    FROM telemetry_updates
    WHERE engine_temperature > 220
    
    UNION ALL
    
    -- Low fuel alerts
    SELECT 
        vehicle_id,
        'LOW_FUEL' as alert_type,
        CASE WHEN fuel_level < 10 THEN 'CRITICAL' ELSE 'WARNING' END as severity,
        'Fuel level: ' || fuel_level || '%' as message
    FROM telemetry_updates
    WHERE fuel_level < 20
    
    UNION ALL
    
    -- Speeding alerts
    SELECT 
        vehicle_id,
        'SPEEDING' as alert_type,
        'WARNING' as severity,
        'Speed: ' || speed_mph || ' mph' as message
    FROM telemetry_updates
    WHERE speed_mph > 75
)
SELECT * FROM alerts;

ALTER TASK vehicle_monitoring_alerts RESUME;