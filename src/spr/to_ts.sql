create function spr.to_ts (
    tz timestamp with time zone default clock_timestamp()
)
    returns bigint
    language sql
    security definer
    stable
as $$
    select trunc(extract(epoch from tz))::bigint
$$;
