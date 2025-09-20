-- dbt macros for stream processing and real-time analytics

{% macro create_stream_on_table(table_name, stream_name=none) %}
  {%- if stream_name is none -%}
    {%- set stream_name = table_name ~ '_stream' -%}
  {%- endif -%}
  
  CREATE OR REPLACE STREAM {{ stream_name }} ON TABLE {{ ref(table_name) }}
  COMMENT = 'Stream for real-time {{ table_name }} updates';
{% endmacro %}

{% macro process_stream_changes(stream_name, target_table, unique_key=none) %}
  {%- if unique_key is none -%}
    {%- set unique_key = 'id' -%}
  {%- endif -%}
  
  MERGE INTO {{ ref(target_table) }} AS target
  USING (
    SELECT * FROM {{ stream_name }}
    WHERE METADATA$ACTION IN ('INSERT', 'UPDATE')
  ) AS source
  ON target.{{ unique_key }} = source.{{ unique_key }}
  WHEN MATCHED AND source.METADATA$ACTION = 'UPDATE' THEN
    UPDATE SET *
  WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    INSERT *
{% endmacro %}

{% macro get_stream_metadata() %}
  SELECT 
    METADATA$ACTION,
    METADATA$ISUPDATE,
    METADATA$ROW_ID,
    METADATA$INSERT_TIMESTAMP,
    METADATA$UPDATE_TIMESTAMP
{% endmacro %}

{% macro incremental_stream_filter(stream_name, timestamp_column='_ingested_at') %}
  {% if is_incremental() %}
    WHERE {{ timestamp_column }} > (
      SELECT COALESCE(MAX({{ timestamp_column }}), '1900-01-01') 
      FROM {{ this }}
    )
  {% endif %}
{% endmacro %}

{% macro real_time_aggregation(stream_name, group_by_columns, metrics) %}
  WITH stream_data AS (
    SELECT 
      {{ group_by_columns | join(', ') }},
      {{ metrics | join(', ') }},
      METADATA$ACTION
    FROM {{ stream_name }}
    WHERE METADATA$ACTION IN ('INSERT', 'UPDATE')
  )
  SELECT 
    {{ group_by_columns | join(', ') }},
    {{ metrics | join(', ') }},
    CURRENT_TIMESTAMP() as processed_at
  FROM stream_data
  GROUP BY {{ group_by_columns | join(', ') }}
{% endmacro %}

{% macro stream_health_check(stream_name) %}
  SELECT 
    '{{ stream_name }}' as stream_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN METADATA$ACTION = 'INSERT' THEN 1 END) as inserts,
    COUNT(CASE WHEN METADATA$ACTION = 'UPDATE' THEN 1 END) as updates,
    COUNT(CASE WHEN METADATA$ACTION = 'DELETE' THEN 1 END) as deletes,
    MIN(METADATA$INSERT_TIMESTAMP) as oldest_record,
    MAX(METADATA$INSERT_TIMESTAMP) as newest_record
  FROM {{ stream_name }}
{% endmacro %}
