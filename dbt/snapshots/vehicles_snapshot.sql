{% snapshot vehicles_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='vehicle_id',
    strategy='check',
    check_cols=['vehicle_type','make','model','capacity_kg','status']
  )
}}

select * from {{ ref('dim_vehicle') }}

{% endsnapshot %}

