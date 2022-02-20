-- truncate a timestamp float
--
create function spr.trunc_tz(
    tz timestamp with time zone,
    type text default 'min2')
returns timestamp with time zone
as $$
    select case
    when type='min'
    then date_trunc('minute', tz)

    when type='min2'
    then date_trunc('hour', tz)
        + date_part('minute', tz)::int / 2 * interval '2 min'

    when type='hourly' or type ='hour'
    then date_trunc('hour', tz)

    -- '300' = 5 minutes
    else to_timestamp(floor(extract(epoch from tz) / type::float) * type::float)
    end
$$ language sql stable;


create function spr.trunc_ts(
    ts bigint,
    int_ts float default 120.0)
returns bigint
as $$
    select floor(ts / int_ts) * int_ts
$$ language sql stable;
