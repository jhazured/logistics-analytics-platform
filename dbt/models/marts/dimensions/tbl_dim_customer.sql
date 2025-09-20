with c as (
  select * from {{ ref('tbl_stg_customers') }}
)
select
  "customer_id",
  {{ dbt_utils.generate_surrogate_key(['"customer_id"']) }} as customer_sk,
  "customer_name",
  "customer_type" as segment,  -- Map customer_type to segment
  'AUSTRALIA' as country,  -- Default country since we're generating Australian data
  "industry_code",
  "credit_limit" as credit_limit_usd,
  "payment_terms",
  to_date("customer_since") as customer_since_date,  -- Convert Unix timestamp to date
  case when "status" = 'ACTIVE' then true else false end as is_active,
  "contact_email",
  "contact_phone",
  "account_manager",
  -- Generate customer_tier based on credit_limit for tests
  case 
    when "credit_limit" >= 1000000 then 'PREMIUM'
    when "credit_limit" >= 100000 then 'STANDARD'
    when "credit_limit" >= 10000 then 'BASIC'
    else 'BASIC'
  end as customer_tier,
  to_date("customer_since") as customer_since
from c
