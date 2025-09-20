-- dbt/macros/error_handling.sql

{% macro safe_divide_with_error_handling(numerator, denominator, default_value=0, error_message='Division by zero') %}
  CASE 
    WHEN {{ denominator }} IS NULL OR {{ denominator }} = 0 THEN 
      {% if var('log_errors', true) %}
        {{ log(error_message ~ " - numerator: " ~ numerator ~ ", denominator: " ~ denominator, info=true) }}
      {% endif %}
      {{ default_value }}
    ELSE {{ numerator }} / {{ denominator }}
  END
{% endmacro %}

{% macro safe_cast_with_error_handling(column, target_type, default_value=NULL) %}
  TRY_CAST({{ column }} AS {{ target_type }})
  {% if default_value is not none %}
  COALESCE(TRY_CAST({{ column }} AS {{ target_type }}), {{ default_value }})
  {% endif %}
{% endmacro %}

{% macro validate_data_quality(table_name, column_name, validation_rule) %}
  {% if execute %}
    {% set query %}
      SELECT COUNT(*) as error_count
      FROM {{ table_name }}
      WHERE NOT ({{ validation_rule }})
    {% endset %}
    
    {% set results = run_query(query) %}
    {% if results[0][0] > 0 %}
      {% do log("Data quality issue in " ~ table_name ~ "." ~ column_name ~ ": " ~ results[0][0] ~ " records failed validation", info=true) %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro log_model_execution_stats(model_name, row_count, execution_time) %}
  {% if execute %}
    {% do log("Model " ~ model_name ~ " completed: " ~ row_count ~ " rows processed in " ~ execution_time ~ " seconds", info=true) %}
  {% endif %}
{% endmacro %}

{% macro handle_missing_data(table_name, key_column, expected_count) %}
  {% if execute %}
    {% set query %}
      SELECT COUNT(*) as actual_count
      FROM {{ table_name }}
    {% endset %}
    
    {% set results = run_query(query) %}
    {% set actual_count = results[0][0] %}
    
    {% if actual_count < expected_count %}
      {% do log("WARNING: " ~ table_name ~ " has only " ~ actual_count ~ " records, expected at least " ~ expected_count, info=true) %}
    {% endif %}
  {% endif %}
{% endmacro %}
