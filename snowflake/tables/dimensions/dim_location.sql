-- Location Dimension
CREATE OR REPLACE TABLE dim_location (
    location_id NUMBER PRIMARY KEY,
    location_name VARCHAR(200) NOT NULL,
    location_type VARCHAR(50) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(10) NOT NULL,
    postcode VARCHAR(10),
    latitude FLOAT,
    longitude FLOAT,
    capacity_rating VARCHAR(20),
    operating_hours VARCHAR(50),
    created_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (state, city);