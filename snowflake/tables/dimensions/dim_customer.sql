-- Customer Dimension
CREATE OR REPLACE TABLE dim_customer (
    customer_id NUMBER PRIMARY KEY,
    customer_name VARCHAR(200) NOT NULL,
    customer_type VARCHAR(50) NOT NULL,
    volume_segment VARCHAR(50) NOT NULL,
    industry VARCHAR(100),
    preferred_delivery_window VARCHAR(20),
    service_level VARCHAR(20) NOT NULL,
    credit_rating VARCHAR(20),
    payment_terms VARCHAR(20),
    signup_date DATE,
    last_order_date DATE,
    total_lifetime_value NUMBER(12,2),
    average_order_value NUMBER(10,2),
    delivery_flexibility_score NUMBER(3,1),
    satisfaction_score NUMBER(3,1),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (volume_segment, customer_type);