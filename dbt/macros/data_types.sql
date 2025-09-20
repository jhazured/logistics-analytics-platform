-- =====================================================
-- Data Type Macros for Logistics Analytics
-- =====================================================

-- Macro for safe casting with error handling
{% macro safe_cast(column, target_type) %}
  TRY_CAST({{ column }} AS {{ target_type }})
{% endmacro %}

-- Macro for safe decimal conversion
{% macro safe_decimal(column, precision=10, scale=2) %}
  TRY_TO_DECIMAL({{ column }}, {{ precision }}, {{ scale }})
{% endmacro %}

-- Macro for safe number conversion
{% macro safe_number(column) %}
  TRY_TO_NUMBER({{ column }})
{% endmacro %}

-- Macro for safe date conversion
{% macro safe_date(column) %}
  TRY_TO_DATE({{ column }})
{% endmacro %}

-- Macro for safe timestamp conversion
{% macro safe_timestamp(column) %}
  TRY_TO_TIMESTAMP({{ column }})
{% endmacro %}

-- Macro for safe varchar conversion
{% macro safe_varchar(column, length=255) %}
  TRY_CAST({{ column }} AS VARCHAR({{ length }}))
{% endmacro %}

-- Macro for converting lbs to kg
{% macro lbs_to_kg(lbs_column) %}
  {{ safe_decimal(lbs_column) }} / 2.20462
{% endmacro %}

-- Macro for converting kg to lbs
{% macro kg_to_lbs(kg_column) %}
  {{ safe_decimal(kg_column) }} * 2.20462
{% endmacro %}

-- Macro for converting miles to km
{% macro miles_to_km(miles_column) %}
  {{ safe_decimal(miles_column) }} * 1.60934
{% endmacro %}

-- Macro for converting km to miles
{% macro km_to_miles(km_column) %}
  {{ safe_decimal(km_column) }} / 1.60934
{% endmacro %}

-- Macro for converting cubic feet to cubic meters
{% macro cubic_feet_to_cubic_meters(cubic_feet_column) %}
  {{ safe_decimal(cubic_feet_column) }} / 35.3147
{% endmacro %}

-- Macro for converting cubic meters to cubic feet
{% macro cubic_meters_to_cubic_feet(cubic_meters_column) %}
  {{ safe_decimal(cubic_meters_column) }} * 35.3147
{% endmacro %}

-- Macro for converting mph to kmh
{% macro mph_to_kmh(mph_column) %}
  {{ safe_decimal(mph_column) }} * 1.60934
{% endmacro %}

-- Macro for converting kmh to mph
{% macro kmh_to_mph(kmh_column) %}
  {{ safe_decimal(kmh_column) }} / 1.60934
{% endmacro %}

-- Macro for converting hours to minutes
{% macro hours_to_minutes(hours_column) %}
  {{ safe_number(hours_column) }} * 60
{% endmacro %}

-- Macro for converting minutes to hours
{% macro minutes_to_hours(minutes_column) %}
  {{ safe_number(minutes_column) }} / 60.0
{% endmacro %}

-- Macro for converting Fahrenheit to Celsius
{% macro fahrenheit_to_celsius(fahrenheit_column) %}
  ({{ safe_decimal(fahrenheit_column) }} - 32) * 5/9
{% endmacro %}

-- Macro for converting Celsius to Fahrenheit
{% macro celsius_to_fahrenheit(celsius_column) %}
  {{ safe_decimal(celsius_column) }} * 9/5 + 32
{% endmacro %}
