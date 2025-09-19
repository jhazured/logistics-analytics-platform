-- Fact Vehicle Utilization
CREATE OR REPLACE TABLE fact_vehicle_utilization (
    utilization_id NUMBER AUTOINCREMENT PRIMARY KEY,
    vehicle_id VARCHAR(20) NOT NULL,
    date_key NUMBER(8) NOT NULL,
    total_distance_km NUMBER(8,1),
    total_runtime_hours NUMBER(5,1),
    capacity_utilized_percent NUMBER(5,2),
    fuel_consumed_liters NUMBER(8,2),
    maintenance_hours NUMBER(4,1),
    downtime_hours NUMBER(4,1),
    utilization_score NUMBER(3,1),
    efficiency_score NUMBER(3,1),
    cost_per_km NUMBER(8,4),
    revenue_per_km NUMBER(8,4),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
)
CLUSTER BY (date_key, vehicle_id);