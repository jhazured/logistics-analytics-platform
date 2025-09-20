{% snapshot locations_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='location_id',
    strategy='check',
    check_cols=['latitude', 'longitude', 'service_area', 'location_type', 'address']
  )
}}

select * from {{ ref('tbl_dim_location') }}

{% endsnapshot %}
