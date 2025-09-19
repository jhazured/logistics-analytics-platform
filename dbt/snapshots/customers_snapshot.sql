{% snapshot customers_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=['customer_name','segment','country']
  )
}}

select * from {{ ref('dim_customer') }}

{% endsnapshot %}

