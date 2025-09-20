-- Customer Dimension (ML-Optimized)
-- This table is materialized from dbt model: dim_customer
CREATE OR REPLACE TABLE MARTS.TBL_DIM_CUSTOMER (
    customer_id NUMBER PRIMARY KEY,
    customer_sk VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    segment VARCHAR(50) NOT NULL,
    country VARCHAR(50) NOT NULL,
    industry_code VARCHAR(50),
    credit_limit_usd NUMBER(15,2),
    payment_terms VARCHAR(20),
    customer_since_date DATE,
    is_active VARCHAR(10) NOT NULL,
    contact_email VARCHAR(200),
    contact_phone VARCHAR(50),
    account_manager VARCHAR(100),
    customer_tier VARCHAR(20),
    customer_since DATE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (customer_tier, segment, is_active);