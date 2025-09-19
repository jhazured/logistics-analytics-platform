-- Vehicle Dimension
CREATE OR REPLACE TABLE dim_vehicle (
    vehicle_id VARCHAR(20) PRIMARY KEY,
    vehicle_type VARCHAR(50) NOT NULL,
    make VARCHAR(50),
    model VARCHAR(100),
    year NUMBER(4),
    capacity_kg NUMBER(10),
    fuel_type VARCHAR(20),
    fuel_efficiency_l_100km NUMBER(5,1),
    purchase_date DATE,
    last_service_date DATE,
    next_service_due DATE,
    odometer_km NUMBER(10),
    condition_score NUMBER(3,1),
    maintenance_cost_ytd NUMBER(10,2),
    is_active BOOLEAN DEFAULT TRUE,
    gps_enabled BOOLEAN DEFAULT FALSE,
    telematics_enabled BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (vehicle_type, is_active);