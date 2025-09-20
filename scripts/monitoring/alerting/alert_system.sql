-- Real-time vehicle tracking and alerts
CREATE OR REPLACE TABLE LOGISTICS_DW_PROD.MONITORING.REAL_TIME_VEHICLE_ALERTS (
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
INSERT INTO LOGISTICS_DW_PROD.MONITORING.REAL_TIME_VEHICLE_ALERTS (vehicle_id, alert_type, severity, message)
WITH telemetry_updates AS (
    SELECT 
        vehicle_id,
        engine_temp_c,
        fuel_level_percent,
        speed_kmh,
        latitude,
        longitude,
        timestamp
    FROM vehicle_telemetry_stream
    WHERE METADATA$ACTION = 'INSERT'
),
alerts AS (
    -- Engine temperature alerts (convert C to F for comparison)
    SELECT 
        vehicle_id,
        'ENGINE_OVERHEATING' as alert_type,
        CASE WHEN engine_temp_c > 100 THEN 'CRITICAL' ELSE 'WARNING' END as severity,
        'Engine temperature: ' || engine_temp_c || '°C' as message
    FROM telemetry_updates
    WHERE engine_temp_c > 90
    
    UNION ALL
    
    -- Low fuel alerts
    SELECT 
        vehicle_id,
        'LOW_FUEL' as alert_type,
        CASE WHEN fuel_level_percent < 10 THEN 'CRITICAL' ELSE 'WARNING' END as severity,
        'Fuel level: ' || fuel_level_percent || '%' as message
    FROM telemetry_updates
    WHERE fuel_level_percent < 20
    
    UNION ALL
    
    -- Speeding alerts (convert kmh to mph for comparison)
    SELECT 
        vehicle_id,
        'SPEEDING' as alert_type,
        'WARNING' as severity,
        'Speed: ' || speed_kmh || ' km/h' as message
    FROM telemetry_updates
    WHERE speed_kmh > 120
)
SELECT * FROM alerts;

ALTER TASK vehicle_monitoring_alerts RESUME;