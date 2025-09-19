-- Create streams for real-time change data capture
CREATE OR REPLACE STREAM shipments_stream ON TABLE fact_shipments
COMMENT = 'Stream for real-time shipment updates';

CREATE OR REPLACE STREAM vehicle_telemetry_stream ON TABLE fact_vehicle_telemetry
COMMENT = 'Stream for real-time vehicle telemetry updates';

CREATE OR REPLACE STREAM route_performance_stream ON TABLE fact_route_performance
COMMENT = 'Stream for real-time route performance updates';
