-- =====================================================
-- dbt Macros for Smart Logistics Analytics Platform
-- File: macros/logistics_macros.sql
-- =====================================================

-- Macro for cost calculation
{% macro calculate_cost_per_unit(total_cost, volume, unit='km') %}
  CASE 
    WHEN {{ volume }} > 0 THEN {{ total_cost }} / {{ volume }}
    ELSE NULL
  END
{% endmacro %}

-- Macro for calculating rolling counts
{% macro rolling_count(partition_by, order_by, windows=[7, 30, 90]) %}
  {% for window in windows %}
    COUNT(*) OVER (
      PARTITION BY {{ partition_by }}
      ORDER BY {{ order_by }}
      ROWS BETWEEN {{ window - 1 }} PRECEDING AND CURRENT ROW
    ) AS count_{{ window }}d
    {%- if not loop.last -%},{%- endif %}
  {% endfor %}
{% endmacro %}

-- Macro for calculating performance score
{% macro calculate_performance_score(on_time_rate, efficiency_rate, satisfaction_score) %}
  CASE 
    WHEN {{ on_time_rate }} IS NULL OR {{ efficiency_rate }} IS NULL OR {{ satisfaction_score }} IS NULL 
    THEN NULL
    ELSE ROUND(
      ({{ on_time_rate }} * 0.4 + 
       {{ efficiency_rate }} * 0.3 + 
       {{ satisfaction_score }} / 10 * 0.3) * 10, 2
    )
  END
{% endmacro %}

-- Macro for haul type classification
{% macro classify_haul_type(distance_km) %}
  CASE 
    WHEN {{ distance_km }} <= 50 THEN 'short_haul'
    WHEN {{ distance_km }} <= 200 THEN 'medium_haul'
    ELSE 'long_haul'
  END
{% endmacro %}

-- Macro for delivery window classification
{% macro classify_delivery_window(planned_date, actual_date) %}
  CASE 
    WHEN DATEDIFF(day, {{ planned_date }}, {{ actual_date }}) = 0 THEN 'same_day'
    WHEN DATEDIFF(day, {{ planned_date }}, {{ actual_date }}) = 1 THEN 'next_day'
    WHEN DATEDIFF(day, {{ planned_date }}, {{ actual_date }}) > 1 THEN 'multi_day'
    ELSE 'unknown'
  END
{% endmacro %}

-- Note: calculate_trend macro is defined in aggregations.sql

-- Note: calculate_volatility macro is defined in aggregations.sql

-- Note: calculate_percentiles macro is defined in aggregations.sql



-- Macro for incremental strategy with late arriving data
{% macro logistics_incremental_strategy() %}
  {% if is_incremental() %}
    WHERE {{ this }}.updated_at > (
      SELECT COALESCE(MAX(updated_at), '1900-01-01'::timestamp) 
      FROM {{ this }}
    )
    OR {{ this }}.shipment_date >= CURRENT_DATE - 7  -- Reprocess last 7 days for late updates
  {% endif %}
{% endmacro %}



-- Macro for ML feature scaling
{% macro scale_feature(column_name, method='standardize') %}
  {% if method == 'standardize' %}
    ({{ column_name }} - AVG({{ column_name }}) OVER ()) / NULLIF(STDDEV({{ column_name }}) OVER (), 0)
  {% elif method == 'normalize' %}
    ({{ column_name }} - MIN({{ column_name }}) OVER ()) / NULLIF(MAX({{ column_name }}) OVER () - MIN({{ column_name }}) OVER (), 0)
  {% elif method == 'robust' %}
    ({{ column_name }} - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {{ column_name }}) OVER ()) / 
    NULLIF(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {{ column_name }}) OVER () - 
           PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {{ column_name }}) OVER (), 0)
  {% endif %}
{% endmacro %}



-- Macro for generating test data ranges
{% macro test_accepted_range(column_name, min_value, max_value) %}
  SELECT *
  FROM {{ this }}
  WHERE {{ column_name }} < {{ min_value }} 
     OR {{ column_name }} > {{ max_value }}
{% endmacro %}

-- =====================================================
-- Advanced Analytics Macros
-- =====================================================

-- Macro for seasonal adjustment
{% macro seasonal_adjustment(value_column, date_column, partition_by=None) %}
  WITH seasonal_factors AS (
    SELECT 
      EXTRACT(month FROM {{ date_column }}) AS month,
      EXTRACT(dayofweek FROM {{ date_column }}) AS dayofweek,
      {% if partition_by %}{{ partition_by }},{% endif %}
      AVG({{ value_column }}) AS seasonal_avg,
      AVG(AVG({{ value_column }})) OVER ({% if partition_by %}PARTITION BY {{ partition_by }}{% endif %}) AS overall_avg
    FROM {{ this }}
    GROUP BY 1, 2{% if partition_by %}, 3{% endif %}
  )
  SELECT 
    *,
    COALESCE(seasonal_avg / NULLIF(overall_avg, 0), 1) AS seasonal_factor,
    {{ value_column }} / COALESCE(seasonal_avg / NULLIF(overall_avg, 0), 1) AS seasonally_adjusted_value
  FROM seasonal_factors
{% endmacro %}

-- Macro for anomaly detection using z-score
{% macro detect_anomalies(column_name, partition_by, threshold=3) %}
  WITH stats AS (
    SELECT 
      *,
      AVG({{ column_name }}) OVER (PARTITION BY {{ partition_by }}) AS mean_value,
      STDDEV({{ column_name }}) OVER (PARTITION BY {{ partition_by }}) AS stddev_value
    FROM {{ this }}
  ),
  z_scores AS (
    SELECT 
      *,
      ABS({{ column_name }} - mean_value) / NULLIF(stddev_value, 0) AS z_score
    FROM stats
  )
  SELECT 
    *,
    CASE WHEN z_score > {{ threshold }} THEN TRUE ELSE FALSE END AS is_anomaly
  FROM z_scores
{% endmacro %}

