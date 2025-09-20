{{ config(
    materialized='view',
    tags=['staging', 'customers']
) }}

with src as (
  select * from {{ source('raw_logistics', 'CUSTOMERS') }}
)
select
  cast("customer_id" as number) as customer_id,
  coalesce(trim("customer_name"), 'UNKNOWN') as customer_name,
  coalesce(trim("customer_type"), 'UNKNOWN') as segment,  -- Map customer_type to segment
  'AUSTRALIA' as country,  -- Default to Australia since we're generating Australian data
  coalesce(trim("industry_code"), 'UNKNOWN') as industry_code,
  cast("credit_limit" as decimal(15,2)) as credit_limit_usd,
  coalesce(trim("payment_terms"), 'NET_30') as payment_terms,
  to_date(to_timestamp_ntz("customer_since", 9)) as customer_since_date,  -- Convert nanoseconds to date
  coalesce(trim("status"), 'ACTIVE') as is_active,
  coalesce(trim("contact_email"), '') as contact_email,
  coalesce(trim("contact_phone"), '') as contact_phone,
  coalesce(trim("account_manager"), 'UNASSIGNED') as account_manager,
  to_timestamp_ntz("created_at", 9) as created_at,  -- Convert nanoseconds to timestamp
  current_timestamp() as _ingested_at
from src
