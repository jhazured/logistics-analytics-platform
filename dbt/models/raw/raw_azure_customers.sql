-- Raw customer data from Azure source system
-- This model extracts and standardizes customer data from Azure

{{ config(
    materialized='view',
    tags=['raw', 'customers', 'azure']
) }}

SELECT 
    customer_id,
    customer_name,
    customer_type,
    industry_code,
    credit_limit,
    payment_terms,
    customer_since,
    status,
    billing_address,
    shipping_address,
    contact_email,
    contact_phone,
    account_manager,
    created_at,
    updated_at,
    _loaded_at
FROM {{ source('raw_logistics', 'customers') }}
WHERE status = 'ACTIVE'
