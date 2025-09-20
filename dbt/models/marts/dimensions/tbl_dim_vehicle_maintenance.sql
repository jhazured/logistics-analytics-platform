{{ config(
    materialized='table',
    tags=['marts', 'dimensions', 'maintenance', 'load_first']
) }}

with maintenance_data as (
    select * from {{ ref('tbl_stg_maintenance_logs') }}
),

vehicles as (
    select * from {{ ref('tbl_dim_vehicle') }}
),

maintenance_enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['maintenance_id']) }} as maintenance_id,
        m.vehicle_id,
        m.maintenance_type,
        m.maintenance_date,
        m.maintenance_mileage,
        m.maintenance_cost_usd,
        m.description,
        m.parts_cost,
        m.labor_cost,
        -- Calculate next maintenance due date
        case 
            when m.maintenance_type = 'ROUTINE' then dateadd(day, 90, m.maintenance_date)
            when m.maintenance_type = 'PREVENTIVE' then dateadd(day, 180, m.maintenance_date)
            when m.maintenance_type = 'INSPECTION' then dateadd(day, 30, m.maintenance_date)
            else dateadd(day, 365, m.maintenance_date)
        end as next_maintenance_due_date,
        -- Calculate next maintenance due mileage
        case 
            when m.maintenance_type = 'ROUTINE' then m.maintenance_mileage + 10000
            when m.maintenance_type = 'PREVENTIVE' then m.maintenance_mileage + 25000
            when m.maintenance_type = 'INSPECTION' then m.maintenance_mileage + 5000
            else m.maintenance_mileage + 50000
        end as next_maintenance_due_mileage,
        -- Determine maintenance status
        case 
            when m.maintenance_date is not null then 'COMPLETED'
            when m.maintenance_date is null and m.maintenance_type = 'SCHEDULED' then 'SCHEDULED'
            when m.maintenance_date is null and m.maintenance_type = 'OVERDUE' then 'OVERDUE'
            else 'IN_PROGRESS'
        end as maintenance_status,
        -- Calculate maintenance risk score
        case 
            when m.maintenance_type = 'EMERGENCY' then 100
            when m.maintenance_type = 'CORRECTIVE' then 80
            when m.maintenance_type = 'PREVENTIVE' then 20
            when m.maintenance_type = 'ROUTINE' then 10
            else 50
        end as risk_score,
        -- Maintenance cost per mile
        case 
            when m.maintenance_mileage > 0 then m.maintenance_cost_usd / m.maintenance_mileage
            else 0
        end as cost_per_mile,
        m._ingested_at as created_at,
        m._ingested_at as updated_at
    from maintenance_data m
)

select * from maintenance_enhanced
