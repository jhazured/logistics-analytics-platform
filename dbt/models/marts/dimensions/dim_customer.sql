with c as (
  select * from {{ ref(stg_customers) }}
)
select
  customer_id,
  {{ dbt_utils.generate_surrogate_key([customer_id]) }} as customer_sk,
  customer_name,
  segment,
  country,
  date_trunc(day, created_at)::date as customer_since
from c
