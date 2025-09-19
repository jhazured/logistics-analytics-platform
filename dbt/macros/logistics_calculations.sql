{% macro minutes_to_hours(minutes_col) -%}
  ({{ minutes_col }} / 60.0)
{%- endmacro %}
