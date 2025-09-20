-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_shipments_customer_date 
ON fact_shipments (customer_id, shipment_date);

CREATE INDEX IF NOT EXISTS idx_shipments_vehicle_date 
ON fact_shipments (vehicle_id, shipment_date);

CREATE INDEX IF NOT EXISTS idx_telemetry_vehicle_time 
ON fact_vehicle_telemetry (vehicle_id, timestamp);

CREATE INDEX IF NOT EXISTS idx_weather_city_date 
ON dim_weather (city, date);
