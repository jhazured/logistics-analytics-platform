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

-- Macro for calculating trend between recent and historical metrics
{% macro calculate_trend(recent_metric, historical_metric) %}
  CASE 
    WHEN {{ historical_metric }} > 0 
    THEN ({{ recent_metric }} / {{ historical_metric }} - 1) * 100
    ELSE NULL
  END
{% endmacro %}

-- Macro for calculating volatility (coefficient of variation)
{% macro calculate_volatility(column_name, partition_by, order_by, window_days) %}
  STDDEV({{ column_name }}) OVER (
    PARTITION BY {{ partition_by }}
    ORDER BY {{ order_by }}
    ROWS BETWEEN {{ window_days - 1 }} PRECEDING AND CURRENT ROW
  ) / NULLIF(AVG({{ column_name }}) OVER (
    PARTITION BY {{ partition_by }}
    ORDER BY {{ order_by }}
    ROWS BETWEEN {{ window_days - 1 }} PRECEDING AND CURRENT ROW
  ), 0)
{% endmacro %}

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

-- Macro for calculating rolling count with multiple windows
{% macro rolling_count(column_name, partition_by, order_by, windows=[7, 30, 90]) %}
  {% for window in windows %}
    COUNT({{ column_name }}) OVER (
      PARTITION BY {{ partition_by }}
      ORDER BY {{ order_by }}
      ROWS BETWEEN {{ window - 1 }} PRECEDING AND CURRENT ROW
    ) AS {{ column_name }}_{{ window }}d_count
    {%- if not loop.last -%},{%- endif %}
  {% endfor %}
{% endmacro %}