-- Real-time aggregation tables for streaming analytics
CREATE OR REPLACE TABLE real_time_kpis (
    kpi_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    metric_name VARCHAR(100),
    metric_value FLOAT,
    dimensions VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Real-time KPI storage for streaming analytics';