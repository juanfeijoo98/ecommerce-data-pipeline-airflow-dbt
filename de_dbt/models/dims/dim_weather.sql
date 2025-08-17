with base as (
    select
        date(ts_utc)                      as date,
        avg(temp_c)::numeric(5,2)         as avg_temp_c,
        min(temp_c)::numeric(5,2)         as min_temp_c,
        max(temp_c)::numeric(5,2)         as max_temp_c,
        avg(rh_pct)::numeric(5,2)         as avg_rh
    from {{ ref('stg_weather') }}
    group by 1
)
select
    to_char(date, 'YYYYMMDD')::int as date_key,
    date,
    avg_temp_c,
    min_temp_c,
    max_temp_c,
    avg_rh
from base
order by date
