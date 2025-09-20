-- Vehicle Dimension (ML-Optimized)
-- This table is materialized from dbt model: dim_vehicle
CREATE OR REPLACE TABLE MARTS.DIM_VEHICLE (
    vehicle_id VARCHAR(20) PRIMARY KEY,
    vehicle_sk VARCHAR(50) NOT NULL,
    vehicle_type VARCHAR(50) NOT NULL,
    capacity_kg NUMBER(10,2),
    capacity_m3 NUMBER(10,3),
    fuel_efficiency_mpg NUMBER(10,3),
    make VARCHAR(50),
    model VARCHAR(100),
    model_year NUMBER(4),
    vehicle_status VARCHAR(20),
    current_mileage NUMBER(12,0),
    last_maintenance_date DATE,
    next_maintenance_date DATE,
    maintenance_interval_miles NUMBER(10,0),
    purchase_price NUMBER(15,2),
    current_value NUMBER(15,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (vehicle_type, vehicle_status);