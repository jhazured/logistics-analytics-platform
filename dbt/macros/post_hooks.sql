-- dbt/macros/post_hooks.sql

{% macro log_table_stats(table) %}
  {% if execute %}
    {% set query %}
      SELECT 
        '{{ table }}' as table_name,
        COUNT(*) as row_count,
        CURRENT_TIMESTAMP() as stats_timestamp
      FROM {{ table }}
    {% endset %}
    
    {% set results = run_query(query) %}
    {% do log("Table stats for " ~ table ~ ": " ~ results, info=true) %}
  {% endif %}
{% endmacro %}

{% macro optimize_clustering(table) %}
  {% if target.name == 'prod' %}
    {% set query %}
      ALTER TABLE {{ table }} 
      RECLUSTER
    {% endset %}
    
    {% do run_query(query) %}
    {% do log("Optimized clustering for " ~ table, info=true) %}
  {% endif %}
{% endmacro %}

{% macro refresh_materialized_view(table) %}
  {% if target.name == 'prod' %}
    {% set query %}
      ALTER MATERIALIZED VIEW {{ table }} 
      REFRESH
    {% endset %}
    
    {% do run_query(query) %}
    {% do log("Refreshed materialized view " ~ table, info=true) %}
  {% endif %}
{% endmacro %}

{% macro update_feature_store_metadata(table) %}
  {% if target.name == 'prod' %}
    {% set query %}
      UPDATE ML_FEATURES.FEATURE_STORE_METADATA 
      SET last_updated = CURRENT_TIMESTAMP(),
          row_count = (SELECT COUNT(*) FROM {{ table }})
      WHERE table_name = '{{ table }}'
    {% endset %}
    
    {% do run_query(query) %}
    {% do log("Updated feature store metadata for " ~ table, info=true) %}
  {% endif %}
{% endmacro %}
