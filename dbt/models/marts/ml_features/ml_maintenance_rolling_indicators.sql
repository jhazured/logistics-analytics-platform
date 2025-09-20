{{ config(materialized='table') }}

with vehicle_telemetry as (
    select * from {{ ref('fact_vehicle_telemetry') }}
),

maintenance_history as (
    select * from {{ ref('dim_vehicle_maintenance') }}
),

vehicles as (
    select * from {{ ref('dim_vehicle') }}
),

telemetry_aggregated as (
    select
        vehicle_id,
        date_trunc('day', timestamp) as telemetry_date,
        avg(engine_temp_c) as avg_engine_temp,
        avg(fuel_level_percent) as avg_fuel_level,
        avg(engine_rpm) as avg_engine_rpm,
        sum(harsh_braking_events) as total_harsh_braking,
        sum(harsh_acceleration_events) as total_harsh_acceleration,
        sum(speeding_events) as total_speeding_events,
        avg(idle_time_minutes) as avg_idle_time,
        avg(engine_health_score) as avg_engine_health,
        count(*) as telemetry_records_count
    from vehicle_telemetry
    group by vehicle_id, date_trunc('day', timestamp)
),

maintenance_rolling as (
    select
        v.vehicle_id,
        v.vehicle_number,
        v.make_model,
        v.model_year,
        v.current_mileage,
        v.maintenance_interval_miles,
        -- Rolling 30-day maintenance indicators
        count(case when m.maintenance_date >= dateadd(day, -30, current_date()) then 1 end) as maintenance_events_30d,
        sum(case when m.maintenance_date >= dateadd(day, -30, current_date()) then m.maintenance_cost_usd else 0 end) as maintenance_cost_30d,
        avg(case when m.maintenance_date >= dateadd(day, -30, current_date()) then m.risk_score else null end) as avg_risk_score_30d,
        -- Rolling 90-day maintenance indicators
        count(case when m.maintenance_date >= dateadd(day, -90, current_date()) then 1 end) as maintenance_events_90d,
        sum(case when m.maintenance_date >= dateadd(day, -90, current_date()) then m.maintenance_cost_usd else 0 end) as maintenance_cost_90d,
        avg(case when m.maintenance_date >= dateadd(day, -90, current_date()) then m.risk_score else null end) as avg_risk_score_90d,
        -- Maintenance urgency indicators
        case 
            when v.current_mileage - coalesce(max(m.maintenance_mileage), 0) > v.maintenance_interval_miles * 1.2 then 'CRITICAL'
            when v.current_mileage - coalesce(max(m.maintenance_mileage), 0) > v.maintenance_interval_miles * 1.1 then 'HIGH'
            when v.current_mileage - coalesce(max(m.maintenance_mileage), 0) > v.maintenance_interval_miles * 1.0 then 'MEDIUM'
            else 'LOW'
        end as maintenance_urgency,
        -- Days since last maintenance
        datediff(day, max(m.maintenance_date), current_date()) as days_since_last_maintenance,
        -- Miles since last maintenance
        v.current_mileage - coalesce(max(m.maintenance_mileage), 0) as miles_since_last_maintenance
    from vehicles v
    left join maintenance_history m on v.vehicle_id = m.vehicle_id
    group by v.vehicle_id, v.vehicle_number, v.make_model, v.model_year, v.current_mileage, v.maintenance_interval_miles
),

telemetry_rolling as (
    select
        vehicle_id,
        -- Rolling 7-day telemetry indicators
        avg(case when telemetry_date >= dateadd(day, -7, current_date()) then avg_engine_temp else null end) as avg_engine_temp_7d,
        avg(case when telemetry_date >= dateadd(day, -7, current_date()) then avg_engine_health else null end) as avg_engine_health_7d,
        sum(case when telemetry_date >= dateadd(day, -7, current_date()) then total_harsh_braking else 0 end) as harsh_braking_7d,
        sum(case when telemetry_date >= dateadd(day, -7, current_date()) then total_harsh_acceleration else 0 end) as harsh_acceleration_7d,
        -- Rolling 30-day telemetry indicators
        avg(case when telemetry_date >= dateadd(day, -30, current_date()) then avg_engine_temp else null end) as avg_engine_temp_30d,
        avg(case when telemetry_date >= dateadd(day, -30, current_date()) then avg_engine_health else null end) as avg_engine_health_30d,
        sum(case when telemetry_date >= dateadd(day, -30, current_date()) then total_harsh_braking else 0 end) as harsh_braking_30d,
        sum(case when telemetry_date >= dateadd(day, -30, current_date()) then total_harsh_acceleration else 0 end) as harsh_acceleration_30d
    from telemetry_aggregated
    group by vehicle_id
)

select
    m.vehicle_id,
    m.vehicle_number,
    m.make_model,
    m.model_year,
    m.current_mileage,
    m.maintenance_interval_miles,
    m.maintenance_events_30d,
    m.maintenance_cost_30d,
    m.avg_risk_score_30d,
    m.maintenance_events_90d,
    m.maintenance_cost_90d,
    m.avg_risk_score_90d,
    m.maintenance_urgency,
    m.days_since_last_maintenance,
    m.miles_since_last_maintenance,
    t.avg_engine_temp_7d,
    t.avg_engine_health_7d,
    t.harsh_braking_7d,
    t.harsh_acceleration_7d,
    t.avg_engine_temp_30d,
    t.avg_engine_health_30d,
    t.harsh_braking_30d,
    t.harsh_acceleration_30d,
    -- Predictive maintenance score
    case 
        when m.maintenance_urgency = 'CRITICAL' then 100
        when m.maintenance_urgency = 'HIGH' then 80
        when m.maintenance_urgency = 'MEDIUM' then 60
        when t.avg_engine_health_7d < 70 then 70
        when t.harsh_braking_7d > 10 or t.harsh_acceleration_7d > 10 then 50
        else 20
    end as predictive_maintenance_score,
    current_date() as feature_date
from maintenance_rolling m
left join telemetry_rolling t on m.vehicle_id = t.vehicle_id
