-- dbt/macros/predictive_maintenance.sql
{% macro maintenance_risk_score(miles_since_maintenance, vehicle_age_years, breakdown_history_count=0) %}
  -- Predictive maintenance risk scoring (0-100)
  LEAST(100, 
    ({{ miles_since_maintenance }} / 10000.0 * 40) +  -- 40% weight for mileage
    ({{ vehicle_age_years }} / 15.0 * 35) +           -- 35% weight for age  
    ({{ breakdown_history_count }} * 5)               -- 25% weight for history
  )
{% endmacro %}

{% macro optimal_maintenance_schedule(vehicle_type, current_mileage, last_maintenance_mileage) %}
  -- Determine optimal maintenance schedule
  CASE 
    WHEN '{{ vehicle_type }}' = 'TRUCK' THEN
      CASE 
        WHEN {{ current_mileage }} - {{ last_maintenance_mileage }} >= 15000 THEN 'OVERDUE'
        WHEN {{ current_mileage }} - {{ last_maintenance_mileage }} >= 12000 THEN 'DUE_SOON'
        WHEN {{ current_mileage }} - {{ last_maintenance_mileage }} >= 10000 THEN 'SCHEDULE_SOON'
        ELSE 'OK'
      END
    WHEN '{{ vehicle_type }}' = 'VAN' THEN
      CASE 
        WHEN {{ current_mileage }} - {{ last_maintenance_mileage }} >= 12000 THEN 'OVERDUE'
        WHEN {{ current_mileage }} - {{ last_maintenance_mileage }} >= 10000 THEN 'DUE_SOON'  
        WHEN {{ current_mileage }} - {{ last_maintenance_mileage }} >= 8000 THEN 'SCHEDULE_SOON'
        ELSE 'OK'
      END
    ELSE
      CASE 
        WHEN {{ current_mileage }} - {{ last_maintenance_mileage }} >= 10000 THEN 'OVERDUE'
        WHEN {{ current_mileage }} - {{ last_maintenance_mileage }} >= 8000 THEN 'DUE_SOON'
        WHEN {{ current_mileage }} - {{ last_maintenance_mileage }} >= 6000 THEN 'SCHEDULE_SOON'
        ELSE 'OK'
      END
  END
{% endmacro %}