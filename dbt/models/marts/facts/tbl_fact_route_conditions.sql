{{ config(
    materialized='incremental',
    unique_key='condition_id',
    on_schema_change='sync_all_columns',
    tags=['marts', 'facts', 'route_conditions', 'load_second']
) }}

with s as (
  select route_id, shipment_date from {{ ref('tbl_fact_shipments') }}
  {% if is_incremental() %}
    where shipment_date > (select coalesce(max(to_date(cast(date_key as string), 'YYYYMMDD')), '1900-01-01') from {{ this }})
  {% endif %}
), d as (
  select date_key, date from {{ ref('tbl_dim_date') }}
)
select
  concat(s.route_id, '_', cast(d.date_key as string)) as condition_id,
  s.route_id,
  d.date_key,
  null as weather_id,
  null as traffic_id,
  3 as weather_impact_score,
  3 as traffic_impact_score,
  3.0 as road_condition_score,
  3.0 as overall_difficulty_score,
  null as recommended_vehicle_types,
  null as estimated_delay_minutes
from s
join d on s.shipment_date = d.date

