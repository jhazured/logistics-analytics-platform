-- Create alert tables for real-time monitoring

-- Real-time vehicle tracking and alerts
CREATE OR REPLACE TABLE LOGISTICS_DW_PROD.MONITORING.TBL_real_time_vehicle_alerts (
    alert_id VARCHAR(50) DEFAULT UUID_STRING(),
    vehicle_id VARCHAR(20),
    alert_type VARCHAR(50),
    severity VARCHAR(20),
    message TEXT,
    alert_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    resolved_at TIMESTAMP_NTZ,
    resolved_by VARCHAR(100)
) COMMENT = 'Real-time vehicle alerts and notifications';

-- System performance alerts
CREATE OR REPLACE TABLE LOGISTICS_DW_PROD.MONITORING.TBL_system_alerts (
    alert_id VARCHAR(50) DEFAULT UUID_STRING(),
    alert_type VARCHAR(50),
    severity VARCHAR(20),
    component VARCHAR(100),
    message TEXT,
    alert_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    resolved_at TIMESTAMP_NTZ,
    resolved_by VARCHAR(100)
) COMMENT = 'System performance and health alerts';

-- Data quality alerts
CREATE OR REPLACE TABLE LOGISTICS_DW_PROD.MONITORING.TBL_data_quality_alerts (
    alert_id VARCHAR(50) DEFAULT UUID_STRING(),
    table_name VARCHAR(255),
    test_name VARCHAR(255),
    severity VARCHAR(20),
    message TEXT,
    alert_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    resolved_at TIMESTAMP_NTZ,
    resolved_by VARCHAR(100)
) COMMENT = 'Data quality test failure alerts';
