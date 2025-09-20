-- Macro for calculating rolling averages with multiple windows
{% macro rolling_average(column_name, partition_by, order_by, windows=[7, 30, 90]) %}
  {% for window in windows %}
    AVG({{ column_name }}) OVER (
      PARTITION BY {{ partition_by }}
      ORDER BY {{ order_by }}
      ROWS BETWEEN {{ window - 1 }} PRECEDING AND CURRENT ROW
    ) AS {{ column_name }}_{{ window }}d_avg
    {%- if not loop.last -%},{%- endif %}
  {% endfor %}
{% endmacro %}

-- Note: calculate_trend macro is defined in aggregations.sql

-- Note: calculate_volatility macro is defined in aggregations.sql

-- Macro for calculating rolling sum with multiple windows
{% macro rolling_sum(column_name, partition_by, order_by, windows=[7, 30, 90]) %}
  {% for window in windows %}
    SUM({{ column_name }}) OVER (
      PARTITION BY {{ partition_by }}
      ORDER BY {{ order_by }}
      ROWS BETWEEN {{ window - 1 }} PRECEDING AND CURRENT ROW
    ) AS {{ column_name }}_{{ window }}d_sum
    {%- if not loop.last -%},{%- endif %}
  {% endfor %}
{% endmacro %}

-- Note: rolling_count macro is defined in cost_calculations.sql