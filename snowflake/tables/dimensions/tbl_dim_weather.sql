-- Weather Dimension
CREATE OR REPLACE TABLE dim_weather (
    weather_id NUMBER PRIMARY KEY,
    date DATE NOT NULL,
    city VARCHAR(100) NOT NULL,
    condition VARCHAR(50),
    temperature_c NUMBER(5,1),
    humidity_percent NUMBER(3),
    wind_speed_kmh NUMBER(5,1),
    precipitation_mm NUMBER(5,1),
    visibility_km NUMBER(5,1),
    weather_severity_score NUMBER(3,1),
    driving_impact_score NUMBER(3,1),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (date, city);
