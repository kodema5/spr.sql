-- get the calculated values and errors of acquisition
--
create type sparing.get_it as (
    logger_id text,
    min_tz timestamp with time zone,
    max_tz timestamp with time zone,
    int_tz interval,

    min_n integer,
    max_n integer,

    has_avg_ph boolean,
    min_avg_ph float,
    max_avg_ph float,

    has_avg_debit boolean,
    max_avg_debit float,

    has_avg_cod boolean,
    max_avg_cod float,
    max_avg_load_cod float,

    has_avg_nh3n boolean,
    max_avg_nh3n float,
    max_avg_load_nh3n float,

    has_avg_tss boolean,
    max_avg_tss float,
    max_avg_load_tss float,

    max_avg_load_total float
);

create type sparing.get_acq_t as (
    id text,
    tz timestamp with time zone,
    n integer,

    avg_ph float, avg_debit float, avg_cod float, avg_nh3n float, avg_tss float,
    min_ph float, min_debit float, min_cod float, min_nh3n float, min_tss float,
    max_ph float, max_debit float, max_cod float, max_nh3n float, max_tss float,
    std_ph float, std_debit float, std_cod  float, std_nh3n  float, std_tss float,
    avg_load_cod float, avg_load_nh3n float, avg_load_tss float, avg_load_total float
);


create function sparing.get_acq_ts ( a sparing.get_it )
    returns setof sparing.get_acq_t
    language sql
    security definer
    stable
as $$
    select *,
        coalesce(tb.avg_load_cod, 0.0)
            + coalesce(tb.avg_load_nh3n, 0.0)
            + coalesce(tb.avg_load_tss, 0.0)
        as avg_load_total
    from (
        select *,
            ta.avg_debit * ta.avg_cod as avg_load_cod,
            ta.avg_debit * ta.avg_nh3n as avg_load_nh3n,
            ta.avg_debit * ta.avg_tss as avg_load_tss
        from
        (
            select
                acq.logger_id,
                sparing.bin_tz(a.int_tz, bin_tz) as tz,
                count(1),
                avg(ph), avg(debit) as avg_debit, avg(cod) as avg_cod, avg(nh3n) as avg_nh3n, avg(tss) as avg_tss,
                min(ph), min(debit), min(cod), min(nh3n), min(tss),
                max(ph), max(debit), max(cod), max(nh3n), max(tss),
                stddev(ph), stddev(debit), stddev(cod), stddev(nh3n), stddev(tss)
            from sparing_.acq
            where logger_id = a.logger_id
            and bin_tz between a.min_tz and a.max_tz
            group by logger_id, bin_tz
        ) ta
    ) tb
$$;


create function sparing.errors (
    p sparing.get_it,
    a sparing.get_acq_t
)
    returns text[]
    language sql
    security definer
    immutable
as $$
    select array_agg(err) from (
        select case when p.min_n is not null and a.n < p.min_n then 'error.below_min_n' else null end
        union
        select case when p.max_n is not null and a.n > p.max_n then 'error.above_max_n' else null end

        union
        select case when p.has_avg_ph and a.avg_ph is null then 'error.missing_avg_ph' else null end
        union
        select case when p.has_avg_ph and a.avg_ph<p.min_avg_ph then 'error.below_min_avg_ph' else null end
        union
        select case when p.has_avg_ph and a.avg_ph>p.max_avg_ph then 'error.above_max_avg_ph' else null end

        union
        select case when p.has_avg_debit and a.avg_debit is null then 'error.missing_avg_debit' else null end
        union
        select case when p.has_avg_debit and a.avg_debit<0 then 'error.below_zero_avg_debit' else null end
        union
        select case when p.has_avg_debit and a.avg_debit>p.max_avg_debit then 'error.above_max_avg_debit' else null end

        union
        select case when p.has_avg_cod and a.avg_cod is null then 'error.missing_avg_cod' else null end
        union
        select case when p.has_avg_cod and a.avg_cod<0 then 'error.below_zero_avg_cod' else null end
        union
        select case when p.has_avg_cod and a.avg_cod>p.max_avg_cod then 'error.above_max_avg_cod' else null end
        union
        select case when p.max_avg_load_cod is not null and a.avg_load_cod is null then 'error.missing_avg_load_cod' else null end
        union
        select case when p.max_avg_load_cod is not null and a.avg_load_cod>p.max_avg_load_cod then 'error.above_max_avg_load_cod' else null end

        union
        select case when p.has_avg_nh3n and a.avg_nh3n is null then 'error.missing_avg_nh3n' else null end
        union
        select case when p.has_avg_nh3n and a.avg_nh3n<0 then 'error.below_zero_avg_nh3n' else null end
        union
        select case when p.has_avg_nh3n and a.avg_nh3n>p.max_avg_nh3n then 'error.above_max_avg_nh3n' else null end
        union
        select case when p.max_avg_load_nh3n is not null and a.avg_load_nh3n is null then 'error.missing_avg_load_nh3n' else null end
        union
        select case when p.max_avg_load_nh3n is not null and a.avg_load_nh3n>p.max_avg_load_nh3n then 'error.above_max_avg_load_nh3n' else null end

        union
        select case when p.has_avg_tss and a.avg_tss is null then 'error.missing_avg_tss' else null end
        union
        select case when p.has_avg_tss and a.avg_tss<0 then 'error.below_zero_avg_tss' else null end
        union
        select case when p.has_avg_tss and a.avg_tss>p.max_avg_tss then 'error.above_max_avg_tss' else null end
        union
        select case when p.max_avg_load_tss is not null and a.avg_load_tss is null then 'error.missing_load_avg_tss' else null end
        union
        select case when p.max_avg_load_tss is not null and a.avg_load_tss>p.max_avg_load_tss then 'error.above_max_avg_load_tss' else null end

        union
        select case when p.max_avg_load_total is not null and a.avg_load_total is null then 'error.missing_avg_load_total' else null end
        union
        select case when p.max_avg_load_total is not null and a.avg_load_total>p.max_avg_load_total then 'error.above_max_avg_load_total' else null end

    ) errs (err)
    where err is not null
$$;


create function sparing.get (a sparing.get_it)
    returns jsonb
    language sql
    security definer
    stable
as $$
    select jsonb_agg(jsonb_strip_nulls(
        to_jsonb(ta.*)
        || jsonb_build_object('errors', sparing.errors(a, ta))
    ))
    from sparing.get_acq_ts(a) ta
$$;


-- this is for web interface
-- expects an array, will return array
--
create function sparing.get (a jsonb)
    returns jsonb
    language sql
    security definer
    stable
as $$
    select jsonb_agg(sparing.get(
        jsonb_populate_record(null::sparing.get_it, ts)
    ))
    from jsonb_array_elements(a) ts
$$;


\if :test
    create function tests.test_get()
        returns setof text
        language plpgsql
    as $$
    declare
        a sparing.get_it;
        r jsonb;
    begin
        insert into sparing_.logger (id)
        values ('aaa');

        insert into sparing_.log (tz, data)
        values
            (timestamp '2010-01-01 15:01:02', '{"id":"aaa","cod":1,"debit":10}'),
            (timestamp '2010-01-01 15:01:03', '{"id":"aaa","cod":2,"debit":10}');

        perform sparing.load_acq();

        a.logger_id = 'aaa';
        a.min_tz = '2010-01-01 00:00:00';
        a.max_tz = '2010-01-02 00:00:00';
        a.int_tz = '1 hour';
        a.has_avg_ph = true;

        r = sparing.get(a);
        return next ok(r[0]['n']::numeric = 1, 'has data');
        return next ok(r[0]['avg_load_cod']::numeric = 15, 'calc avg cod');

        -- for a web interface
        r = sparing.get(to_jsonb(array[a]));
        return next ok(r[0][0]['n']::numeric = 1, 'has data');
    end;
    $$;
\endif
