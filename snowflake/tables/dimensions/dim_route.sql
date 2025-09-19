-- Route Dimension
CREATE OR REPLACE TABLE dim_route (
    route_id NUMBER PRIMARY KEY,
    route_name VARCHAR(200) NOT NULL,
    origin_location_id NUMBER NOT NULL,
    route_type VARCHAR(50) NOT NULL,
    total_distance_km NUMBER(8,1),
    estimated_duration_minutes NUMBER(6),
    number_of_stops NUMBER(3),
    complexity_score NUMBER(3,1),
    traffic_density VARCHAR(20),
    road_quality VARCHAR(20),
    weather_risk VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    created_date DATE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (origin_location_id) REFERENCES dim_location(location_id)
)
CLUSTER BY (origin_location_id, route_type);