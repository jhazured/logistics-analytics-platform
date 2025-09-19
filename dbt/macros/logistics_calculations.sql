-- dbt/macros/logistics_calculations.sql - Enhanced version
{% macro calculate_distance_miles(lat1, lon1, lat2, lon2) %}
  -- Haversine formula for calculating distance between two points
  (
    3959 * ACOS(
      LEAST(1.0, GREATEST(-1.0,
        COS(RADIANS({{ lat1 }})) * COS(RADIANS({{ lat2 }})) * 
        COS(RADIANS({{ lon2 }}) - RADIANS({{ lon1 }})) + 
        SIN(RADIANS({{ lat1 }})) * SIN(RADIANS({{ lat2 }}))
      ))
    )
  )
{% endmacro %}

{% macro calculate_delivery_time_estimate(distance_miles, vehicle_type, traffic_factor=1.0, weather_factor=1.0) %}
  -- Dynamic delivery time calculation based on multiple factors
  CASE 
    WHEN '{{ vehicle_type }}' = 'TRUCK' THEN 
      {{ distance_miles }} / (45.0 * {{ traffic_factor }} * {{ weather_factor }}) * 60  -- minutes
    WHEN '{{ vehicle_type }}' = 'VAN' THEN 
      {{ distance_miles }} / (35.0 * {{ traffic_factor }} * {{ weather_factor }}) * 60  -- minutes  
    WHEN '{{ vehicle_type }}' = 'MOTORCYCLE' THEN 
      {{ distance_miles }} / (25.0 * {{ traffic_factor }} * {{ weather_factor }}) * 60  -- minutes
    ELSE 
      {{ distance_miles }} / (30.0 * {{ traffic_factor }} * {{ weather_factor }}) * 60  -- default
  END
{% endmacro %}

{% macro calculate_fuel_cost(distance_miles, vehicle_type, fuel_price_per_gallon) %}
  -- Fuel cost calculation with vehicle-specific efficiency
  CASE 
    WHEN '{{ vehicle_type }}' = 'TRUCK' THEN 
      {{ distance_miles }} / 8.0 * {{ fuel_price_per_gallon }}  -- 8 MPG for trucks
    WHEN '{{ vehicle_type }}' = 'VAN' THEN 
      {{ distance_miles }} / 12.0 * {{ fuel_price_per_gallon }} -- 12 MPG for vans
    WHEN '{{ vehicle_type }}' = 'MOTORCYCLE' THEN 
      {{ distance_miles }} / 45.0 * {{ fuel_price_per_gallon }} -- 45 MPG for motorcycles
    ELSE 
      {{ distance_miles }} / 15.0 * {{ fuel_price_per_gallon }} -- default 15 MPG
  END
{% endmacro %}

{% macro calculate_carbon_emissions_kg(distance_miles, vehicle_type) %}
  -- Carbon emissions calculation in kg CO2
  CASE 
    WHEN '{{ vehicle_type }}' = 'TRUCK' THEN 
      {{ distance_miles }} * 1.61 * 0.411  -- kg CO2 per km for heavy trucks
    WHEN '{{ vehicle_type }}' = 'VAN' THEN 
      {{ distance_miles }} * 1.61 * 0.281  -- kg CO2 per km for vans
    WHEN '{{ vehicle_type }}' = 'MOTORCYCLE' THEN 
      {{ distance_miles }} * 1.61 * 0.103  -- kg CO2 per km for motorcycles
    ELSE 
      {{ distance_miles }} * 1.61 * 0.251  -- average emissions
  END
{% endmacro %}

{% macro calculate_route_efficiency_score(actual_distance, optimal_distance, actual_time, estimated_time) %}
  -- Route efficiency score (0-100)
  GREATEST(0, 
    100 - (
      (ABS({{ actual_distance }} - {{ optimal_distance }}) / {{ optimal_distance }} * 50) +
      (ABS({{ actual_time }} - {{ estimated_time }}) / {{ estimated_time }} * 50)
    )
  )
{% endmacro %}

{% macro calculate_vehicle_utilization_rate(capacity_used, total_capacity, time_in_use, total_available_time) %}
  -- Comprehensive vehicle utilization rate
  (
    ({{ capacity_used }}::FLOAT / NULLIF({{ total_capacity }}, 0)) * 0.6 +
    ({{ time_in_use }}::FLOAT / NULLIF({{ total_available_time }}, 0)) * 0.4
  ) * 100
{% endmacro %}

{% macro calculate_delivery_priority_score(customer_tier, shipment_value, days_since_order, sla_hours) %}
  -- Dynamic priority scoring for delivery scheduling
  (
    CASE '{{ customer_tier }}'
      WHEN 'PREMIUM' THEN 40
      WHEN 'STANDARD' THEN 25  
      WHEN 'BASIC' THEN 15
      ELSE 20
    END +
    LEAST(30, {{ shipment_value }} / 1000) +
    LEAST(20, {{ days_since_order }} * 5) +
    CASE 
      WHEN {{ sla_hours }} <= 24 THEN 10
      WHEN {{ sla_hours }} <= 48 THEN 5
      ELSE 0
    END
  )
{% endmacro %}

{% macro calculate_cost_per_mile(fuel_cost, driver_cost_per_hour, maintenance_cost_per_mile, delivery_time_hours) %}
  -- Total cost per mile calculation
  (
    {{ fuel_cost }} +
    ({{ driver_cost_per_hour }} * {{ delivery_time_hours }}) +
    {{ maintenance_cost_per_mile }}
  )
{% endmacro %}