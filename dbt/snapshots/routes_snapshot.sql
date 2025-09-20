{% snapshot routes_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='route_id',
    strategy='check',
    check_cols=['distance_km', 'estimated_duration_hours', 'route_type', 'optimization_parameters']
  )
}}

select * from {{ ref('tbl_dim_route') }}

{% endsnapshot %}
