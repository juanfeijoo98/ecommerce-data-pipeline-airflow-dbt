-- Calendario diario entre el min y max de stg_weather
with bounds as (
    select
        date(min(ts_utc)) as start_date,
        date(max(ts_utc)) as end_date
    from {{ ref('stg_weather') }}
),
spine as (
    select
        generate_series(
            (select start_date from bounds),
            (select end_date   from bounds),
            interval '1 day'
        )::date as date_day
)
select
    to_char(date_day, 'YYYYMMDD')::int   as date_key,
    date_day                              as date,
    extract(isodow from date_day)::int    as weekday,
    to_char(date_day, 'Dy')               as weekday_name,
    extract(month from date_day)::int     as month,
    to_char(date_day, 'Mon')              as month_name,
    extract(year from date_day)::int      as year,
    case when extract(isodow from date_day) in (6,7) then true else false end as is_weekend
from spine
order by date_day
