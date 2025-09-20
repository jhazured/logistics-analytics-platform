-- =====================================================
-- Business Logic Macros for Logistics Analytics
-- =====================================================

-- Macro for calculating on-time rate from boolean column
{% macro calculate_on_time_rate(boolean_column) %}
  AVG(CASE WHEN {{ boolean_column }} THEN 1.0 ELSE 0.0 END)
{% endmacro %}

-- Macro for converting customer tier to numeric value
{% macro customer_tier_to_numeric(customer_tier) %}
  CASE 
    WHEN {{ customer_tier }} = 'PREMIUM' THEN 3
    WHEN {{ customer_tier }} = 'STANDARD' THEN 2
    WHEN {{ customer_tier }} = 'BASIC' THEN 1
    ELSE 0
  END
{% endmacro %}

-- Macro for converting vehicle type to numeric value
{% macro vehicle_type_to_numeric(vehicle_type) %}
  CASE 
    WHEN {{ vehicle_type }} = 'TRUCK' THEN 1
    WHEN {{ vehicle_type }} = 'VAN' THEN 2
    WHEN {{ vehicle_type }} = 'MOTORCYCLE' THEN 3
    ELSE 4
  END
{% endmacro %}

-- Macro for safe division with NULLIF
{% macro safe_divide(numerator, denominator, default_value=0) %}
  CASE 
    WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL THEN {{ default_value }}
    ELSE {{ numerator }} / {{ denominator }}
  END
{% endmacro %}

-- Macro for calculating fuel efficiency (km per liter)
{% macro calculate_fuel_efficiency(distance_km, fuel_cost, fuel_price_per_liter) %}
  {{ safe_divide(distance_km, fuel_cost / fuel_price_per_liter, 0) }}
{% endmacro %}

-- Macro for calculating cost per kilometer
{% macro calculate_cost_per_km(total_cost, distance_km) %}
  {{ safe_divide(total_cost, distance_km, 0) }}
{% endmacro %}

-- Macro for calculating speed in km/h
{% macro calculate_speed_kmh(distance_km, duration_minutes) %}
  {{ safe_divide(distance_km, duration_minutes / 60.0, 0) }}
{% endmacro %}

-- Macro for calculating profit margin
{% macro calculate_profit_margin(revenue, total_cost) %}
  {{ safe_divide(revenue - total_cost, revenue, 0) }}
{% endmacro %}

-- Macro for classifying delivery performance
{% macro classify_delivery_performance(on_time_rate) %}
  CASE 
    WHEN {{ on_time_rate }} >= 0.95 THEN 'excellent'
    WHEN {{ on_time_rate }} >= 0.90 THEN 'good'
    WHEN {{ on_time_rate }} >= 0.80 THEN 'acceptable'
    ELSE 'needs_improvement'
  END
{% endmacro %}

-- Macro for classifying customer value
{% macro classify_customer_value(annual_revenue) %}
  CASE 
    WHEN {{ annual_revenue }} >= 1000000 THEN 'high_value'
    WHEN {{ annual_revenue }} >= 100000 THEN 'medium_value'
    WHEN {{ annual_revenue }} >= 10000 THEN 'low_value'
    ELSE 'minimal_value'
  END
{% endmacro %}
