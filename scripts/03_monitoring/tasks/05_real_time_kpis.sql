-- Real-time aggregation tables for streaming analytics
CREATE OR REPLACE TABLE LOGISTICS_DW_PROD.MONITORING.REAL_TIME_KPIS (
    kpi_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    metric_name VARCHAR(100),
    metric_value FLOAT,
    dimensions VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Real-time KPI storage for streaming analytics';