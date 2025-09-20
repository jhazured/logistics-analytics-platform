-- Create streams for real-time change data capture
-- Note: These streams are created on the actual Snowflake tables after dbt deployment
-- Replace LOGISTICS_DW_PROD with your actual database name

-- Stream for shipment updates (matches current fact_shipments schema)
CREATE OR REPLACE STREAM LOGISTICS_DW_PROD.MARTS.SHIPMENTS_STREAM 
ON TABLE LOGISTICS_DW_PROD.MARTS.FACT_SHIPMENTS
COMMENT = 'Stream for real-time shipment updates - captures changes to shipment data';

-- Stream for vehicle telemetry updates (matches current fact_vehicle_telemetry schema)  
CREATE OR REPLACE STREAM LOGISTICS_DW_PROD.MARTS.VEHICLE_TELEMETRY_STREAM 
ON TABLE LOGISTICS_DW_PROD.MARTS.FACT_VEHICLE_TELEMETRY
COMMENT = 'Stream for real-time vehicle telemetry updates - captures IoT sensor data changes';

-- Stream for route performance updates (matches current fact_route_performance schema)
CREATE OR REPLACE STREAM LOGISTICS_DW_PROD.MARTS.ROUTE_PERFORMANCE_STREAM 
ON TABLE LOGISTICS_DW_PROD.MARTS.FACT_ROUTE_PERFORMANCE
COMMENT = 'Stream for real-time route performance updates - captures aggregated route metrics';

-- Additional streams for staging tables (for early change detection)
CREATE OR REPLACE STREAM LOGISTICS_DW_PROD.STAGING.STG_SHIPMENTS_STREAM 
ON TABLE LOGISTICS_DW_PROD.STAGING.STG_SHIPMENTS
COMMENT = 'Stream for staging shipment data changes';

CREATE OR REPLACE STREAM LOGISTICS_DW_PROD.STAGING.STG_VEHICLE_TELEMETRY_STREAM 
ON TABLE LOGISTICS_DW_PROD.STAGING.STG_VEHICLE_TELEMETRY
COMMENT = 'Stream for staging vehicle telemetry data changes';

-- Stream for customer dimension changes
CREATE OR REPLACE STREAM LOGISTICS_DW_PROD.MARTS.CUSTOMER_STREAM 
ON TABLE LOGISTICS_DW_PROD.MARTS.DIM_CUSTOMER
COMMENT = 'Stream for customer dimension changes';

-- Stream for vehicle dimension changes
CREATE OR REPLACE STREAM LOGISTICS_DW_PROD.MARTS.VEHICLE_STREAM 
ON TABLE LOGISTICS_DW_PROD.MARTS.DIM_VEHICLE
COMMENT = 'Stream for vehicle dimension changes';
