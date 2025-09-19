-- Fact Route Conditions
CREATE OR REPLACE TABLE fact_route_conditions (
    condition_id NUMBER AUTOINCREMENT PRIMARY KEY,
    route_id NUMBER NOT NULL,
    date_key NUMBER(8) NOT NULL,
    weather_id NUMBER,
    traffic_id NUMBER,
    road_condition_score NUMBER(3,1),
    weather_impact_score NUMBER(3,1),
    traffic_impact_score NUMBER(3,1),
    overall_difficulty_score NUMBER(3,1),
    recommended_vehicle_types VARIANT,
    estimated_delay_minutes NUMBER(4),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    FOREIGN KEY (route_id) REFERENCES dim_route(route_id),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (weather_id) REFERENCES dim_weather(weather_id),
    FOREIGN KEY (traffic_id) REFERENCES dim_traffic_conditions(traffic_id)
)
CLUSTER BY (date_key, route_id);