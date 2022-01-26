create type spr.get_acq_avgs_t as (
    logger_id text,
    int_ts bigint,
    n int,

    avg_ph float, avg_debit float, avg_cod float, avg_nh3n float, avg_tss float,

    min_ph float, min_debit float, min_cod float, min_nh3n float, min_tss float,

    max_ph float, max_debit float, max_cod float, max_nh3n float, max_tss float,

    std_ph float, std_debit float, std_cod  float, std_nh3n  float, std_tss float
);

create function spr.get_acq_avgs(
    logger_id text,
    acq_min_ts bigint,
    acq_max_ts bigint,
    acq_int_ts bigint
) returns setof spr.get_acq_avgs_t as $$
    select
        acq.logger_id,
        spr.trunc_ts(ts, acq_int_ts) as int_ts,
        count(1),

        avg(ph), avg(debit), avg(cod), avg(nh3n), avg(tss),

        min(ph), min(debit), min(cod), min(nh3n), min(tss),

        max(ph), max(debit), max(cod), max(nh3n), max(tss),

        stddev(ph), stddev(debit), stddev(cod), stddev(nh3n), stddev(tss)
    from spr_.acq
    where logger_id = get_acq_avgs.logger_id
    and ts between acq_min_ts and acq_max_ts
    group by acq.logger_id, int_ts
$$ language sql stable;

create type spr.get_acq_calcs_t as (
    ph float,
    debit float,
    cod float,
    nh3n float,
    tss float,
    load_cod float,
    load_nh3n float,
    load_tss float,
    load_total float,
    errors text[]
);

create function spr.get_acq_calcs(
    logger spr_.logger,
    avg spr.get_acq_avgs_t
) returns spr.get_acq_calcs_t as $$
declare
    a spr.get_acq_calcs_t;
begin
    if logger.eval_method = 'mid' then
        a.ph = (avg.min_ph + avg.max_ph) / 2.0;
        a.debit = (avg.min_debit + avg.max_debit) / 2.0;
        a.cod = (avg.min_cod + avg.max_cod) / 2.0;
        a.nh3n = (avg.min_nh3n + avg.max_nh3n) / 2.0;
        a.tss = (avg.min_tss + avg.max_tss) / 2.0;

    else -- by default use avg
        a.ph = avg.avg_ph;
        a.debit = avg.avg_debit;
        a.cod = avg.avg_cod;
        a.nh3n = avg.avg_nh3n;
        a.tss = avg.avg_tss;
    end if;


    a.load_cod = a.debit * a.cod;
    a.load_nh3n = a.debit * a.nh3n;
    a.load_tss = a.debit * a.tss;
    a.load_total = coalesce(a.load_cod, 0.0) + coalesce(a.load_nh3n, 0.0) + coalesce(a.load_tss, 0.0);

    a.errors = spr.errors(
        logger,
        avg.n, a.ph, a.debit,
        a.cod, a.nh3n, a.tss,
        a.load_cod, a.load_nh3n, a.load_tss, a.load_total
    );
    return a;
end;
$$ language plpgsql;


create type spr.web_acq_get_it as (
    logger_ids text[],
    logger_name text,
    logger_tags text[],
    acq_min_ts bigint,
    acq_max_ts bigint,
    acq_int_ts bigint
);

create function spr.web_acq_get(req jsonb) returns jsonb as $$
declare
    it spr.web_acq_get_it = jsonb_populate_record(null::spr.web_acq_get_it, spr.auth(req));
    ls spr_.logger[];
    l spr_.logger;
    res jsonb = jsonb_build_object();
begin
    select array_agg(rs) into ls from spr.get_loggers(req) rs;

    select coalesce(it.acq_min_ts, min(ts)), coalesce(it.acq_max_ts, max(ts))
    into it.acq_min_ts, it.acq_max_ts
    from only spr_.acq;

    foreach l in array ls loop
        res = res || jsonb_build_object(
            l.id,(
                select jsonb_agg(jsonb_strip_nulls(to_jsonb(rs) || to_jsonb(spr.get_acq_calcs(l, rs))))
                from spr.get_acq_avgs(l.id, it.acq_min_ts, it.acq_max_ts, it.acq_int_ts) rs
            )
        );
    end loop;
    return jsonb_build_object(
        'loggers', ls,
        'acqs', res
    );
end;
$$ language plpgsql;


\if :test
    create function tests.test_spr_web_acq_get() returns setof text as $$
    declare
        sid jsonb = tests.session_as_admin();
        a jsonb;
        ts bigint = spr.trunc_ts(spr_.to_ts(current_timestamp));
    begin
        insert into spr_.acq (logger_id, ts, ph) values
            ('dev1', ts, 1.0),
            ('dev1', ts + 60, 2.0)
            ;

        a = spr.web_acq_get(sid || jsonb_build_object(
            'logger_ids', array['dev1']
        ));
        -- raise warning '---%', jsonb_pretty(a);

        return next ok(true, 'hello');
    end;
    $$ language plpgsql;
\endif