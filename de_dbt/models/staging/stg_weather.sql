select
    cast(ts_utc as timestamp)    as ts_utc,
    cast(temp_c as numeric(5,2)) as temp_c,
    cast(rh_pct as int)          as rh_pct,
    lat,
    lon
from public.weather_hourly
