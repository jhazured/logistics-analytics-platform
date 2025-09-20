-- Date Dimension
CREATE OR REPLACE TABLE dim_date (
    date_key NUMBER(8) PRIMARY KEY,
    date DATE NOT NULL,
    year NUMBER(4) NOT NULL,
    quarter VARCHAR(2) NOT NULL,
    month NUMBER(2) NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week_of_year NUMBER(2) NOT NULL,
    day_of_year NUMBER(3) NOT NULL,
    day_of_month NUMBER(2) NOT NULL,
    day_of_week NUMBER(1) NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_business_day BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    season VARCHAR(10) NOT NULL,
    logistics_day_type VARCHAR(20) NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (date_key);