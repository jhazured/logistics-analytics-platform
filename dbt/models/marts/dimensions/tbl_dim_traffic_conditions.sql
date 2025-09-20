{{ config(materialized='table') }}

with traffic_data as (
    select * from {{ source('raw_logistics', 'traffic') }}
),

routes as (
    select * from {{ ref('tbl_dim_route') }}
),

traffic_enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['traffic_id']) }} as traffic_id,
        t.route_id,
        t.traffic_date,
        t.time_of_day,
        t.traffic_volume,
        t.congestion_level,
        t.average_speed_kmh,
        t.average_speed_kmh * 0.621371 as average_speed_mph,
        -- Calculate delay factor based on congestion and speed
        case 
            when t.congestion_level < 20 then 1.0
            when t.congestion_level < 40 then 1.2
            when t.congestion_level < 60 then 1.5
            when t.congestion_level < 80 then 2.0
            else 2.5
        end as delay_factor,
        -- Determine if this is peak hours
        case 
            when t.time_of_day in ('MORNING_RUSH', 'EVENING_RUSH') then true
            else false
        end as is_peak_hours,
        -- Traffic severity classification
        case 
            when t.congestion_level < 30 and t.average_speed_kmh > 50 then 'LOW'
            when t.congestion_level < 60 and t.average_speed_kmh > 30 then 'MEDIUM'
            when t.congestion_level < 80 and t.average_speed_kmh > 20 then 'HIGH'
            else 'CRITICAL'
        end as traffic_severity,
        t.created_at,
        t.updated_at
    from traffic_data t
)

select * from traffic_enhanced
