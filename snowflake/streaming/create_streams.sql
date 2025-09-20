-- Create streams for real-time change data capture
-- Note: These streams will be created after dbt models are deployed
-- Streams are created on the actual Snowflake tables, not dbt models

-- Stream for shipment updates (matches current fact_shipments schema)
CREATE OR REPLACE STREAM shipments_stream ON TABLE {{ ref('fact_shipments') }}
COMMENT = 'Stream for real-time shipment updates - captures changes to shipment data';

-- Stream for vehicle telemetry updates (matches current fact_vehicle_telemetry schema)  
CREATE OR REPLACE STREAM vehicle_telemetry_stream ON TABLE {{ ref('fact_vehicle_telemetry') }}
COMMENT = 'Stream for real-time vehicle telemetry updates - captures IoT sensor data changes';

-- Stream for route performance updates (matches current fact_route_performance schema)
CREATE OR REPLACE STREAM route_performance_stream ON TABLE {{ ref('fact_route_performance') }}
COMMENT = 'Stream for real-time route performance updates - captures aggregated route metrics';

-- Additional streams for staging tables (for early change detection)
CREATE OR REPLACE STREAM stg_shipments_stream ON TABLE {{ ref('stg_shipments') }}
COMMENT = 'Stream for staging shipment data changes';

CREATE OR REPLACE STREAM stg_vehicle_telemetry_stream ON TABLE {{ ref('stg_vehicle_telemetry') }}
COMMENT = 'Stream for staging vehicle telemetry data changes';
