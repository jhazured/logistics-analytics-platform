-- Source database table creation scripts
-- This file contains DDL statements to create tables in the source database

-- 1. Customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    customer_email VARCHAR(255),
    customer_phone VARCHAR(20),
    customer_address TEXT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(50),
    customer_zip VARCHAR(10),
    customer_country VARCHAR(50),
    customer_tier VARCHAR(20) DEFAULT 'STANDARD',
    industry_vertical VARCHAR(50),
    credit_limit_usd DECIMAL(15,2) DEFAULT 0,
    customer_since_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    payment_terms_days INT DEFAULT 30,
    preferred_delivery_window VARCHAR(50),
    special_handling_requirements TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Vehicles table
CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id INT PRIMARY KEY,
    vehicle_number VARCHAR(50) UNIQUE NOT NULL,
    vehicle_type VARCHAR(20) NOT NULL,
    make_model VARCHAR(100) NOT NULL,
    model_year INT NOT NULL,
    capacity_lbs INT NOT NULL,
    fuel_efficiency_mpg DECIMAL(5,2),
    maintenance_interval_miles INT DEFAULT 10000,
    current_mileage INT DEFAULT 0,
    vehicle_status VARCHAR(20) DEFAULT 'ACTIVE',
    purchase_date DATE,
    last_maintenance_date DATE,
    last_maintenance_mileage INT,
    insurance_expiry_date DATE,
    registration_expiry_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Shipments table
CREATE TABLE IF NOT EXISTS shipments (
    shipment_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    vehicle_id INT,
    route_id INT,
    shipment_date DATE NOT NULL,
    planned_delivery_date DATE,
    actual_delivery_date DATE,
    origin_location_id INT,
    destination_location_id INT,
    weight_kg DECIMAL(10,2),
    volume_m3 DECIMAL(10,2),
    distance_km DECIMAL(10,2),
    planned_duration_minutes INT,
    actual_duration_minutes INT,
    fuel_cost DECIMAL(10,2),
    delivery_cost DECIMAL(10,2),
    revenue DECIMAL(12,2),
    delivery_status VARCHAR(20) DEFAULT 'PENDING',
    priority_level VARCHAR(10) DEFAULT 'MEDIUM',
    service_type VARCHAR(20) DEFAULT 'STANDARD',
    customer_rating INT CHECK (customer_rating >= 1 AND customer_rating <= 5),
    special_instructions TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (vehicle_id) REFERENCES vehicles(vehicle_id)
);

-- 4. Maintenance records table
CREATE TABLE IF NOT EXISTS maintenance_records (
    maintenance_id INT PRIMARY KEY,
    vehicle_id INT NOT NULL,
    maintenance_type VARCHAR(20) NOT NULL,
    maintenance_date DATE NOT NULL,
    maintenance_mileage INT NOT NULL,
    maintenance_cost_usd DECIMAL(10,2),
    maintenance_duration_hours DECIMAL(5,2),
    description TEXT,
    parts_replaced TEXT,
    labor_hours DECIMAL(5,2),
    mechanic_name VARCHAR(100),
    maintenance_status VARCHAR(20) DEFAULT 'COMPLETED',
    next_maintenance_due_date DATE,
    next_maintenance_due_mileage INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (vehicle_id) REFERENCES vehicles(vehicle_id)
);

-- 5. Routes table
CREATE TABLE IF NOT EXISTS routes (
    route_id INT PRIMARY KEY,
    origin_location_id INT NOT NULL,
    destination_location_id INT NOT NULL,
    distance_miles DECIMAL(10,2) NOT NULL,
    estimated_travel_time_hours DECIMAL(5,2) NOT NULL,
    route_type VARCHAR(20) DEFAULT 'MIXED',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. Locations table
CREATE TABLE IF NOT EXISTS locations (
    location_id INT PRIMARY KEY,
    location_name VARCHAR(255) NOT NULL,
    location_type VARCHAR(20) NOT NULL,
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state_province VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    service_area VARCHAR(100),
    delivery_zone VARCHAR(50),
    operating_hours VARCHAR(100),
    contact_person VARCHAR(100),
    contact_phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 7. Weather data table
CREATE TABLE IF NOT EXISTS weather_data (
    weather_id INT PRIMARY KEY,
    location_id INT NOT NULL,
    weather_date DATE NOT NULL,
    temperature_c DECIMAL(5,2),
    precipitation_mm DECIMAL(8,2),
    visibility_km DECIMAL(8,2),
    wind_speed_kmh DECIMAL(8,2),
    weather_condition VARCHAR(50),
    humidity_percent DECIMAL(5,2),
    pressure_hpa DECIMAL(8,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (location_id) REFERENCES locations(location_id)
);

-- 8. Traffic data table
CREATE TABLE IF NOT EXISTS traffic_data (
    traffic_id INT PRIMARY KEY,
    route_id INT NOT NULL,
    traffic_date DATE NOT NULL,
    time_of_day VARCHAR(20),
    traffic_volume VARCHAR(20),
    congestion_level INT CHECK (congestion_level >= 0 AND congestion_level <= 100),
    average_speed_kmh DECIMAL(8,2),
    delay_minutes INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (route_id) REFERENCES routes(route_id)
);

-- 9. Telematics data table
CREATE TABLE IF NOT EXISTS telematics_data (
    telemetry_id INT PRIMARY KEY,
    vehicle_id INT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    speed_kmh DECIMAL(8,2),
    fuel_level_percent DECIMAL(5,2),
    engine_rpm INT,
    engine_temp_c DECIMAL(5,2),
    odometer_km DECIMAL(12,2),
    fuel_consumption_lph DECIMAL(8,2),
    harsh_braking_events INT DEFAULT 0,
    harsh_acceleration_events INT DEFAULT 0,
    speeding_events INT DEFAULT 0,
    idle_time_minutes INT DEFAULT 0,
    diagnostic_codes VARCHAR(500),
    engine_health_score INT CHECK (engine_health_score >= 0 AND engine_health_score <= 100),
    maintenance_alert VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (vehicle_id) REFERENCES vehicles(vehicle_id)
);

-- 10. Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_shipments_customer_id ON shipments(customer_id);
CREATE INDEX IF NOT EXISTS idx_shipments_vehicle_id ON shipments(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_shipments_shipment_date ON shipments(shipment_date);
CREATE INDEX IF NOT EXISTS idx_maintenance_vehicle_id ON maintenance_records(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_maintenance_date ON maintenance_records(maintenance_date);
CREATE INDEX IF NOT EXISTS idx_telematics_vehicle_id ON telematics_data(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_telematics_timestamp ON telematics_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_weather_location_date ON weather_data(location_id, weather_date);
CREATE INDEX IF NOT EXISTS idx_traffic_route_date ON traffic_data(route_id, traffic_date);

-- 11. Create triggers for updated_at timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_customers_updated_at BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_vehicles_updated_at BEFORE UPDATE ON vehicles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_shipments_updated_at BEFORE UPDATE ON shipments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_maintenance_updated_at BEFORE UPDATE ON maintenance_records
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_routes_updated_at BEFORE UPDATE ON routes
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_locations_updated_at BEFORE UPDATE ON locations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_weather_updated_at BEFORE UPDATE ON weather_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_traffic_updated_at BEFORE UPDATE ON traffic_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_telematics_updated_at BEFORE UPDATE ON telematics_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
