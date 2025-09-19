{% macro rolling_efficiency_metrics(metric_column, partition_by, order_by, window_days) %}
  -- Rolling efficiency calculations for logistics KPIs
  AVG({{ metric_column }}) OVER (
    PARTITION BY {{ partition_by }}
    ORDER BY {{ order_by }}
    ROWS BETWEEN {{ window_days - 1 }} PRECEDING AND CURRENT ROW
  ) as avg_{{ window_days }}d,
  
  MIN({{ metric_column }}) OVER (
    PARTITION BY {{ partition_by }}
    ORDER BY {{ order_by }}
    ROWS BETWEEN {{ window_days - 1 }} PRECEDING AND CURRENT ROW
  ) as min_{{ window_days }}d,
  
  MAX({{ metric_column }}) OVER (
    PARTITION BY {{ partition_by }}
    ORDER BY {{ order_by }}
    ROWS BETWEEN {{ window_days - 1 }} PRECEDING AND CURRENT ROW
  ) as max_{{ window_days }}d,
  
  STDDEV({{ metric_column }}) OVER (
    PARTITION BY {{ partition_by }}
    ORDER BY {{ order_by }}
    ROWS BETWEEN {{ window_days - 1 }} PRECEDING AND CURRENT ROW
  ) as stddev_{{ window_days }}d
{% endmacro %}

{% macro calculate_seasonal_adjustment_factor(date_column, metric_column) %}
  -- Seasonal adjustment for logistics demand patterns
  {{ metric_column }} / 
  NULLIF(
    AVG({{ metric_column }}) OVER (
      PARTITION BY 
        EXTRACT(MONTH FROM {{ date_column }}),
        EXTRACT(DOW FROM {{ date_column }})
    ), 0
  )
{% endmacro %}

{% macro detect_delivery_anomalies(actual_time, estimated_time, threshold_pct=25) %}
  -- Anomaly detection for delivery times
  CASE 
    WHEN ABS({{ actual_time }} - {{ estimated_time }}) / {{ estimated_time }} > {{ threshold_pct / 100 }}
    THEN 'ANOMALY'
    ELSE 'NORMAL'
  END
{% endmacro %}

{% macro calculate_route_optimization_savings(current_distance, optimal_distance, fuel_cost_per_mile) %}
  -- Calculate potential savings from route optimization
  CASE 
    WHEN {{ current_distance }} > {{ optimal_distance }}
    THEN ({{ current_distance }} - {{ optimal_distance }}) * {{ fuel_cost_per_mile }}
    ELSE 0
  END
{% endmacro %}