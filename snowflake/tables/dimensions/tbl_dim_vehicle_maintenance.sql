-- Vehicle Maintenance Dimension
CREATE OR REPLACE TABLE TBL_DIM_VEHICLE_MAINTENANCE (
    maintenance_id NUMBER AUTOINCREMENT PRIMARY KEY,
    vehicle_id VARCHAR(20) NOT NULL,
    maintenance_type VARCHAR(50) NOT NULL,
    scheduled_date DATE,
    completed_date DATE,
    cost NUMBER(10,2),
    description TEXT,
    service_provider VARCHAR(200),
    next_service_km NUMBER(10),
    priority_level VARCHAR(20),
    status VARCHAR(20),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id)
)
CLUSTER BY (vehicle_id, scheduled_date);
