with src as (
  select * from {{ source(raw, azure_customers) }}
)
select
  cast(customer_id as number) as customer_id,
  coalesce(trim(customer_name), UNKNOWN) as customer_name,
  coalesce(trim(segment), UNKNOWN) as segment,
  coalesce(trim(country), UNKNOWN) as country,
  try_to_timestamp(created_at) as created_at,
  current_timestamp() as _ingested_at
from src
