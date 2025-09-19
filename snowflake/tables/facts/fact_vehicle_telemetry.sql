-- Fact Vehicle Telemetry
CREATE OR REPLACE TABLE fact_vehicle_telemetry (
    telemetry_id NUMBER PRIMARY KEY,
    vehicle_id VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP_NTZ NOT NULL,
    latitude FLOAT,
    longitude FLOAT,
    speed_kmh NUMBER(5,1),
    fuel_level_percent NUMBER(3),
    engine_rpm NUMBER(5),
    engine_temp_c NUMBER(5,1),
    odometer_km NUMBER(10),
    fuel_consumption_lph NUMBER(5,2),
    harsh_braking_events NUMBER(3),
    harsh_acceleration_events NUMBER(3),
    speeding_events NUMBER(3),
    idle_time_minutes NUMBER(5),
    diagnostic_codes VARIANT,
    engine_health_score NUMBER(3,1),
    maintenance_alert BOOLEAN,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id)
)
CLUSTER BY (DATE(timestamp), vehicle_id);