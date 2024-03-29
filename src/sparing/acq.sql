-- sparing_.logger contains id to filter "junk" log
--
create table if not exists sparing_.logger (
    id text not null
        default md5(uuid_generate_v4()::text)
        primary key,
    created_tz timestamp with time zone
        default current_timestamp
);


-- sparing_.acq contains loaded log
-- trim_acq let table contains last 1 day worth of data
-- else partitioned to acq_(logger_id)
--
create table if not exists sparing_.acq (
    logger_id text
        references sparing_.logger(id)
        on delete cascade,
    bin_tz timestamp with time zone,
    primary key (logger_id, bin_tz),

    n int, -- count
    debit float, -- m3/h = 1000L/h
    ph float, -- 0-14
    cod float, -- mg/L
    nh3n float, -- mg/L
    tss float -- mg/L
);


-- returns the lower_tz of a date of interval
--
create function sparing.bin_tz (
    stride_ interval,
    tz_ timestamp with time zone
        default current_timestamp,
    base_tz_ timestamp with time zone
        default '2000-01-01'
)
    returns timestamp with time zone
    language sql
    immutable
as $$
select
    base_tz_
    + floor(extract(epoch from tz_ - base_tz_) / extract(epoch from stride_))::bigint
    * stride_;
$$;

-- is an unrolled type between log->acq
--
create type sparing.log_t as (
    logger_id text,
    bin_tz timestamp with time zone,
    debit float,
    ph float,
    cod float,
    nh3n float,
    tss float
);


create function sparing.log_t (
    log sparing_.log
)
    returns sparing.log_t
    language sql
    security definer
    immutable
as $$
    select (
        log.data->>'id',
        sparing.bin_tz(
            '2 minutes',  -- log is binned at 2 minutes
            log.tz),
        log.data->>'debit',
        log.data->>'ph',
        log.data->>'cod',
        log.data->>'nh3n',
        log.data->>'tss'
    )::sparing.log_t
$$;


-- loads log into acq
--
create function sparing.load_acq (
    min_tz_ timestamp with time zone,
    max_tz_ timestamp with time zone
)
    returns int
    language sql
    security definer
as $$
    with
    -- remove errand rows first
    --
    deleted as (
        delete from sparing_.acq
        where bin_tz between min_tz_ and max_tz_
    ),
    inserted as (
        insert into sparing_.acq
            select
                logger_id,
                bin_tz,
                count(1) as n,
                avg(debit) as debit,
                avg(ph) as ph,
                avg(cod) as cod,
                avg(nh3n) as nh3n,
                avg(tss) as tss
            from (
                select (sparing.log_t(l)).*
                from sparing_.log l
                where tz >= min_tz_
                and tz <= max_tz_
            ) t
            group by logger_id, bin_tz
        returning 1
    )
    select count(1) from inserted
$$;


-- checks unloaded log in ONLY sparing_.log table (not its partitions)
-- to be loaded to acq table
--
create function sparing.load_acq ()
    returns int
    language sql
    security definer
as $$
    with
    updated as (
        update only sparing_.log
        set loaded = true
        where not loaded
        returning *
    ),
    minmax_tz as (
        select min(tz) as min_tz, max(tz) as max_tz
        from updated
    )
    select sparing.load_acq(
        min_tz_ := mm.min_tz,
        max_tz_ := mm.max_tz
    )
    from minmax_tz as mm
$$;


create function sparing.acq_partition_name(
    logger_id_ text
)
    returns text
    language sql
    security definer
    immutable
as $$
    select 'acq_' || logger_id_;
$$;


create function sparing.create_acq_partition(
    logger_id_ text
)
    returns void
    language plpgsql
    security definer
as $$
declare
    tn text = sparing.acq_partition_name(logger_id_);
begin
    if sparing.has_table(tn)
    then
        return;
    end if;

    raise warning 'creating sparing_.%', tn;

    execute format('
        create table sparing_.%I (
            primary key(logger_id),
            check(logger_id=''%s'')
        ) inherits (sparing_.acq)
        ',
        tn,
        logger_id_
    );
end;
$$;

-- trim older than 1-day acq table
-- partition trims by logger_id
--
-- sparing.get retrieves by each logger id,
-- at most hit 2 tables: acq and acq_(logger_id) tables
--
-- query for 1-day old acq hits trimmed acq table only
--
create function sparing.trim_acq ()
    returns int
    language plpgsql
    security definer
as $$
declare
    r record;
    ls text[];
    l text;
    max_tz timestamp with time zone
        = clock_timestamp() - interval '1 day';
    n int = 0;
    i int;
begin
    -- prepare tables
    --
    for r in
        select distinct logger_id
        from only sparing_.acq
        where bin_tz < max_tz
        order by logger_id asc
    loop
        ls = ls || r.logger_id;
        perform sparing.create_acq_partition(
            r.logger_id
        );
    end loop;

    if ls is null
    then
        return 0;
    end if;

    -- partition by logger_id
    --
    foreach l in array ls
    loop
        execute format('
            with
            deleted as (
                delete from only sparing_.acq
                where
                    bin_tz < ''%s''::timestamp with time zone
                    and logger_id=''%s''
                returning *
            ),
            inserted as (
                insert into sparing_.%s
                select * from deleted
                returning 1
            )
            select count(1) from inserted
        ',
            max_tz,
            l,
            sparing.acq_partition_name(l)
        )
        into i;

        n = n + 1;
    end loop;

    return n;
end;
$$;


\if :test
    create function tests.test_acq ()
        returns setof text
        language plpgsql
    as $$
    declare
        a sparing_.acq;
        n int;
    begin
        insert into sparing_.logger (id)
        values ('aaa');

        insert into sparing_.log (tz, data)
        values
            (date_trunc('day', current_timestamp) + interval '1 hour', '{"id":"aaa","ph":3}'),
            (timestamp '2010-01-01 15:01:02', '{"id":"aaa","ph":1}'),
            (timestamp '2010-01-01 15:01:03', '{"id":"aaa","ph":2}');

        n = sparing.load_acq();
        return next ok(n=2, 'inserts 2 acqs');
        n = sparing.load_acq();
        return next ok(n=0, 'skips loaded log');

        n = sparing.trim_acq();
        return next ok(n=1, '1 old acq trimmed');
        n = sparing.trim_acq();
        return next ok(n=0, 'no more old acq to be trimmed');

        select * into a from only sparing_.acq limit 1;
        return next ok(a.n=1, 'acq has most recent');

        select * into a from sparing_.acq_aaa limit 1;
        return next ok(a.n=2, 'can group');
        return next ok(a.ph=1.5, 'can average');
    end;
    $$;
\endif
