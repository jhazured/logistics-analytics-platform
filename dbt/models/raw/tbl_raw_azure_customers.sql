-- Raw customer data from Azure source system
-- This model extracts and standardizes customer data from Azure
-- Uses incremental loading to minimize Fivetran costs

{{ config(
    materialized='incremental',
    unique_key='customer_id',
    merge_update_columns=['customer_name', 'customer_type', 'industry_code', 'credit_limit', 'payment_terms', 'customer_since', 'status', 'billing_address', 'shipping_address', 'contact_email', 'contact_phone', 'account_manager', 'created_at', 'updated_at', '_loaded_at'],
    tags=['raw', 'customers', 'azure', 'incremental']
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

{% if is_incremental() %}
    -- Only process records that are new or updated since last run
    AND _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}
