-- Macro for generating date spine
{% macro generate_date_spine(start_date, end_date) %}
  WITH date_spine AS (
    SELECT 
      DATEADD(day, ROW_NUMBER() OVER (ORDER BY NULL) - 1, '{{ start_date }}'::date) AS date
    FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF(day, '{{ start_date }}'::date, '{{ end_date }}'::date) + 1))
  )
  SELECT * FROM date_spine
{% endmacro %}