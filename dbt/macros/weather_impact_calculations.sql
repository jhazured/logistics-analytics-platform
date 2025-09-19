-- dbt/macros/weather_impact_calculations.sql
{% macro weather_impact_factor(weather_condition, precipitation_mm=0, temperature_f=70) %}
  -- Calculate weather impact on delivery performance
  CASE 
    WHEN '{{ weather_condition }}' IN ('SNOW', 'ICE', 'BLIZZARD') THEN 0.4
    WHEN '{{ weather_condition }}' IN ('HEAVY_RAIN', 'THUNDERSTORM') THEN 0.6
    WHEN '{{ weather_condition }}' = 'RAIN' AND {{ precipitation_mm }} > 10 THEN 0.7
    WHEN '{{ weather_condition }}' = 'FOG' THEN 0.75
    WHEN {{ temperature_f }} < 20 OR {{ temperature_f }} > 100 THEN 0.8
    WHEN '{{ weather_condition }}' IN ('CLOUDY', 'LIGHT_RAIN') THEN 0.9
    ELSE 1.0  -- Clear conditions
  END
{% endmacro %}