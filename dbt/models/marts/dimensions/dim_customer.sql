with c as (
  select * from {{ ref('stg_customers') }}
)
select
  customer_id,
  {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
  customer_name,
  segment,
  country,
  industry_code,
  credit_limit_usd,
  payment_terms,
  customer_since_date,
  is_active,
  contact_email,
  contact_phone,
  account_manager,
  -- Generate customer_tier based on credit_limit_usd for tests
  case 
    when credit_limit_usd >= 1000000 then 'PREMIUM'
    when credit_limit_usd >= 100000 then 'STANDARD'
    when credit_limit_usd >= 10000 then 'BASIC'
    else 'BASIC'
  end as customer_tier,
  date_trunc('day', created_at)::date as customer_since
from c
