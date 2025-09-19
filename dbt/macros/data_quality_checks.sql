-- Macro for data quality checks
{% macro check_data_freshness(table_name, timestamp_column, max_hours=6) %}
  SELECT 
    '{{ table_name }}' AS table_name,
    MAX({{ timestamp_column }}) AS latest_timestamp,
    DATEDIFF(hour, MAX({{ timestamp_column }}), CURRENT_TIMESTAMP()) AS hours_since_update,
    CASE 
      WHEN DATEDIFF(hour, MAX({{ timestamp_column }}), CURRENT_TIMESTAMP()) > {{ max_hours }}
      THEN 'STALE'
      ELSE 'FRESH'
    END AS freshness_status
  FROM {{ ref(table_name) }}
{% endmacro %}