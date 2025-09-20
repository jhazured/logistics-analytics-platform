-- Sample data insertion scripts for source database
-- This file contains INSERT statements to populate tables with sample data

-- 1. Insert sample customers
INSERT INTO customers (customer_id, customer_name, customer_email, customer_phone, customer_address, customer_city, customer_state, customer_zip, customer_country, customer_tier, industry_vertical, credit_limit_usd, customer_since_date, is_active, payment_terms_days, preferred_delivery_window, special_handling_requirements) VALUES
(1, 'Acme Corporation', 'contact@acme.com', '+1-555-0101', '123 Business Ave', 'New York', 'NY', '10001', 'USA', 'PREMIUM', 'RETAIL', 100000.00, '2020-01-15', TRUE, 30, '9AM-5PM', 'Fragile items require special handling'),
(2, 'TechStart Inc', 'info@techstart.com', '+1-555-0102', '456 Innovation St', 'San Francisco', 'CA', '94105', 'USA', 'STANDARD', 'TECHNOLOGY', 50000.00, '2021-03-20', TRUE, 45, '8AM-6PM', NULL),
(3, 'Global Manufacturing', 'orders@globalmfg.com', '+1-555-0103', '789 Industrial Blvd', 'Detroit', 'MI', '48201', 'USA', 'PREMIUM', 'MANUFACTURING', 200000.00, '2019-06-10', TRUE, 30, '7AM-4PM', 'Heavy machinery parts'),
(4, 'HealthCare Plus', 'logistics@healthcareplus.com', '+1-555-0104', '321 Medical Center Dr', 'Boston', 'MA', '02101', 'USA', 'STANDARD', 'HEALTHCARE', 75000.00, '2022-01-05', TRUE, 30, '9AM-5PM', 'Temperature controlled items'),
(5, 'AutoParts Direct', 'shipping@autoparts.com', '+1-555-0105', '654 Auto Parts Way', 'Chicago', 'IL', '60601', 'USA', 'BASIC', 'AUTOMOTIVE', 25000.00, '2022-08-15', TRUE, 60, '8AM-5PM', NULL);

-- 2. Insert sample vehicles
INSERT INTO vehicles (vehicle_id, vehicle_number, vehicle_type, make_model, model_year, capacity_lbs, fuel_efficiency_mpg, maintenance_interval_miles, current_mileage, vehicle_status, purchase_date, last_maintenance_date, last_maintenance_mileage, insurance_expiry_date, registration_expiry_date) VALUES
(1, 'TRK-001', 'TRUCK', 'Ford F-150', 2020, 3000, 22.5, 10000, 45000, 'ACTIVE', '2020-03-15', '2023-11-15', 44000, '2024-03-15', '2024-03-15'),
(2, 'VAN-002', 'VAN', 'Chevrolet Express', 2021, 2500, 18.2, 10000, 32000, 'ACTIVE', '2021-05-20', '2023-10-20', 31000, '2024-05-20', '2024-05-20'),
(3, 'TRK-003', 'TRUCK', 'Ram 2500', 2019, 4000, 19.8, 10000, 67000, 'ACTIVE', '2019-08-10', '2023-12-01', 66000, '2024-08-10', '2024-08-10'),
(4, 'VAN-004', 'VAN', 'Mercedes Sprinter', 2022, 3000, 25.1, 10000, 18000, 'ACTIVE', '2022-02-28', '2023-09-28', 17000, '2024-02-28', '2024-02-28'),
(5, 'TRK-005', 'TRUCK', 'GMC Sierra', 2020, 3500, 21.3, 10000, 52000, 'MAINTENANCE', '2020-11-05', '2023-12-15', 51000, '2024-11-05', '2024-11-05');

-- 3. Insert sample locations
INSERT INTO locations (location_id, location_name, location_type, address_line1, city, state_province, postal_code, country, latitude, longitude, service_area, delivery_zone, operating_hours, contact_person, contact_phone) VALUES
(1, 'Main Warehouse', 'WAREHOUSE', '1000 Distribution Center Blvd', 'New York', 'NY', '10001', 'USA', 40.7589, -73.9851, 'Northeast', 'NYC-001', '24/7', 'John Smith', '+1-555-1001'),
(2, 'West Coast Hub', 'HUB', '2000 Logistics Way', 'Los Angeles', 'CA', '90001', 'USA', 34.0522, -118.2437, 'West Coast', 'LA-001', '6AM-10PM', 'Maria Garcia', '+1-555-1002'),
(3, 'Central Depot', 'DEPOT', '3000 Central Ave', 'Chicago', 'IL', '60601', 'USA', 41.8781, -87.6298, 'Midwest', 'CHI-001', '5AM-11PM', 'Robert Johnson', '+1-555-1003'),
(4, 'Acme Corp Office', 'CUSTOMER', '123 Business Ave', 'New York', 'NY', '10001', 'USA', 40.7589, -73.9851, 'Northeast', 'NYC-001', '9AM-5PM', 'Sarah Wilson', '+1-555-0101'),
(5, 'TechStart Office', 'CUSTOMER', '456 Innovation St', 'San Francisco', 'CA', '94105', 'USA', 37.7749, -122.4194, 'West Coast', 'SF-001', '8AM-6PM', 'David Chen', '+1-555-0102');

-- 4. Insert sample routes
INSERT INTO routes (route_id, origin_location_id, destination_location_id, distance_miles, estimated_travel_time_hours, route_type, is_active) VALUES
(1, 1, 4, 5.2, 0.5, 'URBAN', TRUE),
(2, 2, 5, 8.7, 1.2, 'URBAN', TRUE),
(3, 1, 3, 789.5, 12.5, 'HIGHWAY', TRUE),
(4, 3, 1, 789.5, 12.5, 'HIGHWAY', TRUE),
(5, 2, 3, 2014.2, 32.0, 'HIGHWAY', TRUE);

-- 5. Insert sample shipments
INSERT INTO shipments (shipment_id, customer_id, vehicle_id, route_id, shipment_date, planned_delivery_date, actual_delivery_date, origin_location_id, destination_location_id, weight_kg, volume_m3, distance_km, planned_duration_minutes, actual_duration_minutes, fuel_cost, delivery_cost, revenue, delivery_status, priority_level, service_type, customer_rating) VALUES
(1, 1, 1, 1, '2024-01-15', '2024-01-15', '2024-01-15', 1, 4, 150.5, 2.3, 8.4, 30, 28, 15.50, 25.00, 150.00, 'DELIVERED', 'HIGH', 'EXPRESS', 5),
(2, 2, 2, 2, '2024-01-15', '2024-01-15', '2024-01-15', 2, 5, 75.2, 1.1, 14.0, 75, 72, 12.30, 20.00, 120.00, 'DELIVERED', 'MEDIUM', 'STANDARD', 4),
(3, 3, 3, 3, '2024-01-14', '2024-01-16', '2024-01-16', 1, 3, 500.0, 8.5, 1270.5, 750, 780, 180.75, 150.00, 800.00, 'DELIVERED', 'HIGH', 'STANDARD', 5),
(4, 4, 4, 1, '2024-01-16', '2024-01-16', NULL, 1, 4, 25.8, 0.5, 8.4, 30, NULL, 8.25, 15.00, 75.00, 'IN_TRANSIT', 'MEDIUM', 'STANDARD', NULL),
(5, 5, 5, 2, '2024-01-16', '2024-01-16', NULL, 2, 5, 200.0, 3.2, 14.0, 75, NULL, 18.50, 30.00, 200.00, 'PENDING', 'LOW', 'STANDARD', NULL);

-- 6. Insert sample maintenance records
INSERT INTO maintenance_records (maintenance_id, vehicle_id, maintenance_type, maintenance_date, maintenance_mileage, maintenance_cost_usd, maintenance_duration_hours, description, parts_replaced, labor_hours, mechanic_name, maintenance_status, next_maintenance_due_date, next_maintenance_due_mileage) VALUES
(1, 1, 'ROUTINE', '2023-11-15', 44000, 150.00, 2.5, 'Regular oil change and inspection', 'Oil filter, Air filter', 2.5, 'Mike Johnson', 'COMPLETED', '2024-02-15', 54000),
(2, 2, 'PREVENTIVE', '2023-10-20', 31000, 300.00, 4.0, 'Brake pad replacement and tire rotation', 'Brake pads, Brake fluid', 4.0, 'Sarah Davis', 'COMPLETED', '2024-04-20', 56000),
(3, 3, 'CORRECTIVE', '2023-12-01', 66000, 800.00, 6.5, 'Transmission repair and engine tune-up', 'Transmission fluid, Spark plugs', 6.5, 'Tom Wilson', 'COMPLETED', '2024-03-01', 76000),
(4, 4, 'ROUTINE', '2023-09-28', 17000, 120.00, 1.5, 'Oil change and basic inspection', 'Oil filter', 1.5, 'Lisa Brown', 'COMPLETED', '2023-12-28', 27000),
(5, 5, 'EMERGENCY', '2023-12-15', 51000, 1200.00, 8.0, 'Engine overheating repair', 'Thermostat, Coolant, Water pump', 8.0, 'Mike Johnson', 'COMPLETED', '2024-03-15', 61000);

-- 7. Insert sample weather data
INSERT INTO weather_data (weather_id, location_id, weather_date, temperature_c, precipitation_mm, visibility_km, wind_speed_kmh, weather_condition, humidity_percent, pressure_hpa) VALUES
(1, 1, '2024-01-15', 5.2, 0.0, 15.0, 12.5, 'CLEAR', 65.0, 1013.2),
(2, 2, '2024-01-15', 18.7, 0.0, 20.0, 8.3, 'CLEAR', 55.0, 1015.8),
(3, 3, '2024-01-15', -2.1, 2.5, 8.0, 15.2, 'CLOUDY', 75.0, 1008.5),
(4, 1, '2024-01-16', 3.8, 5.2, 12.0, 18.7, 'RAIN', 80.0, 1005.3),
(5, 2, '2024-01-16', 16.3, 0.0, 18.0, 10.1, 'PARTLY_CLOUDY', 60.0, 1012.1);

-- 8. Insert sample traffic data
INSERT INTO traffic_data (traffic_id, route_id, traffic_date, time_of_day, traffic_volume, congestion_level, average_speed_kmh, delay_minutes) VALUES
(1, 1, '2024-01-15', 'MORNING_RUSH', 'HIGH', 45, 35.2, 5),
(2, 2, '2024-01-15', 'MIDDAY', 'MEDIUM', 25, 45.8, 2),
(3, 3, '2024-01-14', 'EVENING_RUSH', 'LOW', 15, 65.3, 0),
(4, 1, '2024-01-16', 'MORNING_RUSH', 'VERY_HIGH', 75, 25.1, 12),
(5, 2, '2024-01-16', 'MIDDAY', 'MEDIUM', 30, 42.5, 3);

-- 9. Insert sample telematics data
INSERT INTO telematics_data (telemetry_id, vehicle_id, timestamp, latitude, longitude, speed_kmh, fuel_level_percent, engine_rpm, engine_temp_c, odometer_km, fuel_consumption_lph, harsh_braking_events, harsh_acceleration_events, speeding_events, idle_time_minutes, diagnostic_codes, engine_health_score, maintenance_alert) VALUES
(1, 1, '2024-01-15 08:30:00', 40.7589, -73.9851, 45.2, 85.5, 2200, 88.5, 45000.0, 12.3, 0, 0, 0, 5, NULL, 95, 'INFO'),
(2, 2, '2024-01-15 09:15:00', 34.0522, -118.2437, 38.7, 72.3, 1950, 85.2, 32000.0, 10.8, 1, 0, 0, 8, NULL, 92, 'INFO'),
(3, 3, '2024-01-14 14:20:00', 41.8781, -87.6298, 65.8, 45.2, 2800, 92.1, 67000.0, 15.2, 0, 1, 0, 3, NULL, 88, 'WARNING'),
(4, 4, '2024-01-16 10:45:00', 40.7589, -73.9851, 25.1, 90.1, 1800, 82.3, 18000.0, 8.5, 0, 0, 0, 12, NULL, 96, 'INFO'),
(5, 5, '2024-01-16 11:30:00', 34.0522, -118.2437, 42.5, 68.7, 2100, 89.7, 52000.0, 11.9, 0, 0, 1, 6, 'P0301', 85, 'WARNING');

-- 10. Update sequences for auto-incrementing IDs (if using sequences)
-- Note: This is database-specific and may need adjustment based on your database system
-- For PostgreSQL, you would typically use SERIAL or IDENTITY columns
-- For MySQL, you would use AUTO_INCREMENT
-- For SQL Server, you would use IDENTITY columns
