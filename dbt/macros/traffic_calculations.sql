-- dbt/macros/traffic_calculations.sql  
{% macro traffic_delay_factor(hour_of_day, day_of_week, route_type='URBAN') %}
  -- Traffic delay factors based on time and route type
  CASE 
    WHEN '{{ route_type }}' = 'HIGHWAY' THEN
      CASE 
        WHEN {{ hour_of_day }} BETWEEN 7 AND 9 OR {{ hour_of_day }} BETWEEN 17 AND 19 THEN 0.7
        WHEN {{ hour_of_day }} BETWEEN 10 AND 16 THEN 0.9
        ELSE 1.0
      END
    WHEN '{{ route_type }}' = 'URBAN' THEN
      CASE 
        WHEN {{ day_of_week }} BETWEEN 2 AND 6 THEN  -- Monday to Friday
          CASE 
            WHEN {{ hour_of_day }} BETWEEN 7 AND 10 OR {{ hour_of_day }} BETWEEN 16 AND 19 THEN 0.5
            WHEN {{ hour_of_day }} BETWEEN 11 AND 15 THEN 0.8
            ELSE 0.9
          END
        ELSE 0.95  -- Weekends
      END
    ELSE 0.85  -- Rural/other
  END
{% endmacro %}