-- Fact Route Performance
CREATE OR REPLACE TABLE fact_route_performance (
    performance_id NUMBER AUTOINCREMENT PRIMARY KEY,
    route_id NUMBER NOT NULL,
    date_key NUMBER(8) NOT NULL,
    vehicle_id VARCHAR(20) NOT NULL,
    planned_time_minutes NUMBER(6),
    actual_time_minutes NUMBER(6),
    time_variance_minutes NUMBER(6),
    planned_fuel_cost NUMBER(8,2),
    actual_fuel_cost NUMBER(8,2),
    fuel_variance NUMBER(8,2),
    on_time_deliveries NUMBER(3),
    total_deliveries NUMBER(3),
    on_time_percentage NUMBER(5,2),
    customer_satisfaction_avg NUMBER(3,1),
    weather_delays_minutes NUMBER(5),
    traffic_delays_minutes NUMBER(5),
    mechanical_delays_minutes NUMBER(5),
    performance_score NUMBER(3,1),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    FOREIGN KEY (route_id) REFERENCES dim_route(route_id),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id)
)
CLUSTER BY (date_key, route_id);