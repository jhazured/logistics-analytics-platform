with dates as (
  {{ dbt_utils.date_spine(
       datepart="day",
       start_date="to_date('" ~ var('start_date', '2023-01-01') ~ "')",
       end_date="current_date()"
  ) }}
)
select
  to_number(to_char(date_day, 'YYYYMMDD')) as date_key,
  date_day as date,
  extract(year from date_day) as year,
  extract(quarter from date_day) as quarter,
  extract(month from date_day) as month,
  to_char(date_day, 'YYYY-MM') as year_month,
  to_char(date_day, 'MON') as month_name,
  to_char(date_day, 'DY') as day_name,
  extract(dayofweek from date_day) as day_of_week,
  extract(dayofyear from date_day) as day_of_year,
  case when extract(dayofweek from date_day) in (0, 6) then true else false end as is_weekend
from dates
