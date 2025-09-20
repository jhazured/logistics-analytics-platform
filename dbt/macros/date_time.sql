-- =====================================================
-- Date/Time Macros for Logistics Analytics
-- =====================================================

-- Macro for calculating date N days ago
{% macro days_ago(days) %}
  DATEADD(day, -{{ days }}, CURRENT_DATE())
{% endmacro %}

-- Macro for calculating date N days from now
{% macro days_from_now(days) %}
  DATEADD(day, {{ days }}, CURRENT_DATE())
{% endmacro %}

-- Macro for generating rolling window clause
{% macro rolling_window_days(days) %}
  ROWS BETWEEN {{ days - 1 }} PRECEDING AND CURRENT ROW
{% endmacro %}

-- Macro for extracting date part safely
{% macro extract_date_part(date_column, part) %}
  EXTRACT({{ part }} FROM {{ date_column }})
{% endmacro %}

-- Macro for calculating days between dates
{% macro days_between(start_date, end_date) %}
  DATEDIFF(day, {{ start_date }}, {{ end_date }})
{% endmacro %}

-- Macro for calculating hours between timestamps
{% macro hours_between(start_timestamp, end_timestamp) %}
  DATEDIFF(hour, {{ start_timestamp }}, {{ end_timestamp }})
{% endmacro %}

-- Macro for calculating minutes between timestamps
{% macro minutes_between(start_timestamp, end_timestamp) %}
  DATEDIFF(minute, {{ start_timestamp }}, {{ end_timestamp }})
{% endmacro %}

-- Macro for getting start of month
{% macro start_of_month(date_column) %}
  DATE_TRUNC('month', {{ date_column }})
{% endmacro %}

-- Macro for getting end of month
{% macro end_of_month(date_column) %}
  LAST_DAY({{ date_column }})
{% endmacro %}

-- Macro for getting start of week
{% macro start_of_week(date_column) %}
  DATE_TRUNC('week', {{ date_column }})
{% endmacro %}

-- Macro for checking if date is weekend
{% macro is_weekend(date_column) %}
  EXTRACT(dayofweek FROM {{ date_column }}) IN (0, 6)
{% endmacro %}

-- Macro for checking if date is weekday
{% macro is_weekday(date_column) %}
  EXTRACT(dayofweek FROM {{ date_column }}) BETWEEN 1 AND 5
{% endmacro %}

-- Macro for getting current timestamp
{% macro current_timestamp_utc() %}
  CURRENT_TIMESTAMP()
{% endmacro %}

-- Macro for getting current date
{% macro current_date_utc() %}
  CURRENT_DATE()
{% endmacro %}
