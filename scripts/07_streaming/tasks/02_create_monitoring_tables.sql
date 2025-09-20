-- Create real-time monitoring tables

-- Step 1: Create real-time KPI table
CREATE OR REPLACE TABLE LOGISTICS_DW_PROD.MONITORING.REAL_TIME_KPIS (
    kpi_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    metric_name VARCHAR(100),
    metric_value FLOAT,
    dimensions VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Real-time KPI storage for streaming analytics';

-- Step 2: Create vehicle alerts table
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
