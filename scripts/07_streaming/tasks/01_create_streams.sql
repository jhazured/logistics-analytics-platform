-- Create streams on fact tables
-- Note: Replace {{ ref('table_name') }} with actual table names after dbt deployment

-- Create streams on fact tables
CREATE OR REPLACE STREAM STR_shipments_stream ON TABLE LOGISTICS_ANALYTICS.MARTS.FACT_SHIPMENTS
COMMENT = 'Stream for real-time shipment updates';

CREATE OR REPLACE STREAM STR_vehicle_telemetry_stream ON TABLE LOGISTICS_ANALYTICS.MARTS.FACT_VEHICLE_TELEMETRY
COMMENT = 'Stream for real-time vehicle telemetry updates';

CREATE OR REPLACE STREAM STR_route_performance_stream ON TABLE LOGISTICS_ANALYTICS.MARTS.FACT_ROUTE_PERFORMANCE
COMMENT = 'Stream for real-time route performance updates';

-- Create streams on staging tables for early change detection
CREATE OR REPLACE STREAM STR_stg_shipments_stream ON TABLE LOGISTICS_ANALYTICS.STAGING.STG_SHIPMENTS
COMMENT = 'Stream for staging shipment data changes';

CREATE OR REPLACE STREAM STR_stg_vehicle_telemetry_stream ON TABLE LOGISTICS_ANALYTICS.STAGING.STG_VEHICLE_TELEMETRY
COMMENT = 'Stream for staging vehicle telemetry data changes';