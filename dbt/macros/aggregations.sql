-- =====================================================
-- Aggregation Macros for Logistics Analytics
-- =====================================================

-- Macro for standard daily aggregations
{% macro daily_metrics(partition_by, order_by) %}
  COUNT(*) AS daily_count,
  SUM(weight_kg) AS daily_weight,
  SUM(volume_m3) AS daily_volume,
  SUM(revenue) AS daily_revenue,
  SUM(distance_km) AS daily_distance,
  SUM(fuel_cost) AS daily_fuel_cost,
  SUM(delivery_cost) AS daily_delivery_cost,
  AVG(customer_rating) AS daily_avg_rating,
  {{ calculate_on_time_rate('is_on_time') }} AS daily_on_time_rate,
  COUNT(DISTINCT destination_location_id) AS daily_unique_destinations
{% endmacro %}

-- Macro for rolling metrics with multiple windows
{% macro rolling_metrics(column, partition_by, order_by, windows=[7, 30, 90]) %}
  {% for window in windows %}
    AVG({{ column }}) OVER (
      PARTITION BY {{ partition_by }}
      ORDER BY {{ order_by }}
      {{ rolling_window_days(window) }}
    ) AS {{ column }}_{{ window }}d_avg,
    SUM({{ column }}) OVER (
      PARTITION BY {{ partition_by }}
      ORDER BY {{ order_by }}
      {{ rolling_window_days(window) }}
    ) AS {{ column }}_{{ window }}d_sum,
    COUNT({{ column }}) OVER (
      PARTITION BY {{ partition_by }}
      ORDER BY {{ order_by }}
      {{ rolling_window_days(window) }}
    ) AS {{ column }}_{{ window }}d_count
    {%- if not loop.last -%},{%- endif %}
  {% endfor %}
{% endmacro %}

-- Macro for rolling averages with multiple windows
{% macro rolling_averages(column, partition_by, order_by, windows=[7, 30, 90]) %}
  {% for window in windows %}
    AVG({{ column }}) OVER (
      PARTITION BY {{ partition_by }}
      ORDER BY {{ order_by }}
      {{ rolling_window_days(window) }}
    ) AS {{ column }}_{{ window }}d_avg
    {%- if not loop.last -%},{%- endif %}
  {% endfor %}
{% endmacro %}

-- Macro for rolling sums with multiple windows
{% macro rolling_sums(column, partition_by, order_by, windows=[7, 30, 90]) %}
  {% for window in windows %}
    SUM({{ column }}) OVER (
      PARTITION BY {{ partition_by }}
      ORDER BY {{ order_by }}
      {{ rolling_window_days(window) }}
    ) AS {{ column }}_{{ window }}d_sum
    {%- if not loop.last -%},{%- endif %}
  {% endfor %}
{% endmacro %}

-- Macro for rolling counts with multiple windows
{% macro rolling_counts(column, partition_by, order_by, windows=[7, 30, 90]) %}
  {% for window in windows %}
    COUNT({{ column }}) OVER (
      PARTITION BY {{ partition_by }}
      ORDER BY {{ order_by }}
      {{ rolling_window_days(window) }}
    ) AS {{ column }}_{{ window }}d_count
    {%- if not loop.last -%},{%- endif %}
  {% endfor %}
{% endmacro %}

-- Macro for calculating volatility (coefficient of variation)
{% macro calculate_volatility(column, partition_by, order_by, window_days=30) %}
  STDDEV({{ column }}) OVER (
    PARTITION BY {{ partition_by }}
    ORDER BY {{ order_by }}
    {{ rolling_window_days(window_days) }}
  ) / NULLIF(AVG({{ column }}) OVER (
    PARTITION BY {{ partition_by }}
    ORDER BY {{ order_by }}
    {{ rolling_window_days(window_days) }}
  ), 0)
{% endmacro %}

-- Macro for calculating trend between recent and historical metrics
{% macro calculate_trend(recent_metric, historical_metric) %}
  CASE 
    WHEN {{ historical_metric }} > 0 
    THEN ({{ recent_metric }} / {{ historical_metric }} - 1) * 100
    ELSE NULL
  END
{% endmacro %}

-- Macro for calculating percentiles
{% macro calculate_percentiles(column, partition_by, percentiles=[25, 50, 75, 90]) %}
  {% for percentile in percentiles %}
    PERCENTILE_CONT({{ percentile / 100 }}) WITHIN GROUP (ORDER BY {{ column }}) OVER (
      PARTITION BY {{ partition_by }}
    ) AS {{ column }}_p{{ percentile }}
    {%- if not loop.last -%},{%- endif %}
  {% endfor %}
{% endmacro %}

-- Macro for calculating moving averages
{% macro moving_average(column, partition_by, order_by, window_days) %}
  AVG({{ column }}) OVER (
    PARTITION BY {{ partition_by }}
    ORDER BY {{ order_by }}
    {{ rolling_window_days(window_days) }}
  )
{% endmacro %}

-- Macro for calculating moving sums
{% macro moving_sum(column, partition_by, order_by, window_days) %}
  SUM({{ column }}) OVER (
    PARTITION BY {{ partition_by }}
    ORDER BY {{ order_by }}
    {{ rolling_window_days(window_days) }}
  )
{% endmacro %}

-- Macro for calculating moving counts
{% macro moving_count(column, partition_by, order_by, window_days) %}
  COUNT({{ column }}) OVER (
    PARTITION BY {{ partition_by }}
    ORDER BY {{ order_by }}
    {{ rolling_window_days(window_days) }}
  )
{% endmacro %}
