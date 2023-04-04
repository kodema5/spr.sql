-- sparing_.log has immutable raw-logger data
-- only keeps 1 day worth of data
-- else partitioned daily into log_YYYY_MM_DD
--
create table if not exists sparing_.log (
    id serial,
    tz timestamp with time zone
        default now(),
    data jsonb,
    loaded boolean default false
);

create function sparing.has_table(
    table_name text,
    schema_name text default 'sparing_'
)
    returns boolean
    language sql
    security definer
    stable
as $$
    select exists(
        select 1 from pg_catalog.pg_class c
        join pg_catalog.pg_namespace n on n.oid = c.relnamespace
        where n.nspname = schema_name
        and c.relname = table_name
        and c.relkind = 'r'
    )
$$;


create function sparing.log_partition_name(
    tz_ timestamp with time zone
        default current_timestamp
)
    returns text
    language sql
    security definer
    immutable
as $$
    select 'log_' || to_char(tz_, 'YYYY_MM_DD');
$$;


create function sparing.create_log_partition (
    min_tz_ timestamp with time zone
        default date_trunc('day', current_timestamp - interval '1 day'),
    max_tz_ timestamp with time zone
        default date_trunc('day', current_timestamp)
)
    returns void
    language plpgsql
    security definer
as $$
declare
    tn text = sparing.log_partition_name(min_tz_);
begin
    if sparing.has_table(tn)
    then
        return;
    end if;

    raise warning 'creating sparing_.%', tn;

    execute format('
        create table sparing_.%I (
            primary key(id),
            check(
                tz >= ''%s''::timestamp with time zone
                and tz < ''%s''::timestamp with time zone
            )
        ) inherits (sparing_.log)
        ',
        tn,
        min_tz_,
        max_tz_
    );
end;
$$;

-- trims log_table into log_yyyy_mm_dd partitions
-- leaving it < 1 day worth of data
--
create function sparing.trim_log()
    returns void
    language plpgsql
as $$
declare
    r record;
    ts timestamp with time zone[];
    t timestamp with time zone;
begin
    -- prepare tables
    --
    for r in
        select distinct date_trunc('day', tz) as tz
        from only sparing_.log
        where tz < date_trunc('day', clock_timestamp())
        order by tz desc
    loop
        ts = ts || r.tz;
        perform sparing.create_log_partition (
            r.tz,
            r.tz + interval '1 day'
        );
    end loop;

    foreach t in array ts
    loop
        execute format('
            with
            deleted as (
                delete from only sparing_.log
                where tz >= ''%s''::timestamp with time zone
                and tz < ''%s''::timestamp with time zone
                returning *
            )
            insert into sparing_.%s
            select * from deleted
        ',
            t,
            t + interval '1 day',
            sparing.log_partition_name(t)
        );
    end loop;

end;
$$;


\if :test
    create function tests.test_trim_log()
        returns setof text
        language plpgsql
    as $$
    declare
        r sparing_.log;
        tn text;
        n int;
    begin
        return next ok(
            not sparing.has_table('sparing_.log_2020_01_01'),
            'no partition 2010-01-01');
        return next ok(
            not sparing.has_table('sparing_.log_2020_02_01'),
            'no partition 2010-02-01');

        insert into sparing_.log (tz, data)
        values
            (date_trunc('day', current_timestamp) + interval '1 hour', '{"id":"xxx"}'),
            (timestamp '2010-01-01 15:00:00', '{"id":"aaa"}'),
            (timestamp '2010-02-01 15:00:00', '{"id":"bbb"}'),
            (timestamp '2010-02-01 16:00:00', '{"id":"ccc"}');

        perform sparing.trim_log();

        return next ok(
                (select count(1) from sparing_.log)=4,
                'total log');
        return next ok(
                (select count(1) from only sparing_.log)=1,
                'has most today partition');
        return next ok(
                (select count(1) from sparing_.log_2010_01_01)=1,
                'partition 2010-01-01 created');
        return next ok(
                (select count(1) from sparing_.log_2010_02_01)=2,
                'partition 2010-02-01 created');

    end;
    $$;
\endif
