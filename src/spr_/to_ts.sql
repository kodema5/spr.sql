do $$
begin
    create function spr_.to_ts (
        tz timestamp with time zone default clock_timestamp()
    )
        returns bigint
        language sql
        security definer
        stable
    as '
        select trunc(extract(epoch from tz))::bigint
    ';

exception
    when duplicate_function then
    null;
end; $$;