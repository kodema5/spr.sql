
create table if not exists spr_.log (
    id serial,
    ts bigint default spr_.to_ts(clock_timestamp()),
    data jsonb
);

