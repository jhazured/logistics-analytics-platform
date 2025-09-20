-- Traffic Conditions Dimension
CREATE OR REPLACE TABLE TBL_DIM_TRAFFIC_CONDITIONS (
    traffic_id NUMBER AUTOINCREMENT PRIMARY KEY,
    date DATE NOT NULL,
    hour NUMBER(2) NOT NULL,
    city VARCHAR(100) NOT NULL,
    traffic_level VARCHAR(20) NOT NULL,
    congestion_score NUMBER(3,1),
    average_speed_kmh NUMBER(5,1),
    incident_count NUMBER(3),
    road_closures NUMBER(3),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (date, city);