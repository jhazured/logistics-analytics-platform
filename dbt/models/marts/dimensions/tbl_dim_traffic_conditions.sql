{{ config(
    materialized='table',
    tags=['marts', 'dimensions', 'traffic', 'load_first']
) }}

with traffic_data as (
    select * from {{ ref('tbl_stg_traffic_conditions') }}
),

routes as (
    select * from {{ ref('tbl_dim_route') }}
),

traffic_enhanced as (
    select
        traffic_id,
        t.location_id as route_id,  -- Map location_id to route_id for consistency
        t.date as traffic_date,
        t.hour as time_of_day,
        t.congestion_delay_minutes as traffic_volume,
        t.traffic_level as congestion_level,
        t.average_speed_kmh,
        t.average_speed_kmh * 0.621371 as average_speed_mph,
        -- Calculate delay factor based on congestion and speed
        case 
            when t.congestion_delay_minutes < 5 then 1.0
            when t.congestion_delay_minutes < 15 then 1.2
            when t.congestion_delay_minutes < 30 then 1.5
            when t.congestion_delay_minutes < 60 then 2.0
            else 2.5
        end as delay_factor,
        -- Determine if this is peak hours
        case 
            when t.hour between 7 and 9 or t.hour between 17 and 19 then true
            else false
        end as is_peak_hours,
        -- Traffic severity classification
        case 
            when t.congestion_delay_minutes < 5 and t.average_speed_kmh > 50 then 'LOW'
            when t.congestion_delay_minutes < 15 and t.average_speed_kmh > 30 then 'MEDIUM'
            when t.congestion_delay_minutes < 30 and t.average_speed_kmh > 20 then 'HIGH'
            else 'CRITICAL'
        end as traffic_severity,
        t._ingested_at as created_at,
        t._ingested_at as updated_at
    from traffic_data t
)

select * from traffic_enhanced
