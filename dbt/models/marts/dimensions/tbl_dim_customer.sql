{{ config(
    materialized='table',
    tags=['marts', 'dimensions', 'customer', 'load_first']
) }}

with c as (
  select * from {{ ref('tbl_stg_customers') }}
)
select
  customer_id,
  customer_id as customer_sk,
  customer_name,
  segment as customer_type,  -- Map segment to customer_type
  country,
  industry_code,
  credit_limit_usd,
  payment_terms,
  customer_since_date,
  case when is_active = 'ACTIVE' then true else false end as is_active,
  contact_email,
  contact_phone,
  account_manager,
  -- Generate customer_tier based on credit_limit for tests
  case 
    when credit_limit_usd >= 1000000 then 'PREMIUM'
    when credit_limit_usd >= 100000 then 'STANDARD'
    when credit_limit_usd >= 10000 then 'BASIC'
    else 'BASIC'
  end as customer_tier,
  customer_since_date as customer_since
from c
