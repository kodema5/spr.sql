create type spr_admin.acq_t as (
    logger_id text,
    ts bigint,
    ph float,
    cod float,   -- mg/L
    nh3n float,  -- mg/L
    tss float,   -- mg/L
    debit float
);

create function spr_admin.to_acq (r spr_.log)
returns spr_admin.acq_t
as $$
    select (
        (r.data)->>'id',
        r.ts,
        ((r.data)->>'ph')::float,
        ((r.data)->>'cod')::float,
        ((r.data)->>'nh3n')::float,
        ((r.data)->>'tss')::float,
        ((r.data)->>'debit')::float
    )::spr_admin.acq_t;
$$ language sql stable;


create type spr_admin.web_acq_put_it as (
    _auth spr.auth_t,
    log_min_ts bigint,
    log_max_ts bigint,
    log_int_ts int
);

create type spr_admin.web_acq_put_t as (
    count int,
    logger_count int,
    log_min_ts bigint,
    log_max_ts bigint
);

create function spr_admin.web_acq_put (
    it spr_admin.web_acq_put_it)
returns spr_admin.web_acq_put_t
as $$
declare
    a spr_admin.web_acq_put_t;
begin
    select coalesce(it.log_min_ts, min(ts)),
        coalesce(it.log_max_ts, max(ts))
    into it.log_min_ts, it.log_max_ts
    from only spr_.log;

    it.log_int_ts = coalesce(it.log_int_ts, 120);

    with
    selected as (
        select * from spr_.log
        where ts between it.log_min_ts and it.log_max_ts
    ),
    raw_acqs as (
        select
            logger_id,
            spr.trunc_ts(ts) as per_ts,
            count(1) as n,
            avg(ph) as ph,
            avg(debit) as debit,
            avg(cod) as cod,
            avg(nh3n) as nh3n,
            avg(tss) as tss
        from (
            select (spr_admin.to_acq(ds::spr_.log)).*
            from selected ds
        ) as rs
        group by logger_id, per_ts
    ),
    acqs as (
        select rs.*,
            rs.debit * rs.cod as load_cod,
            rs.debit * rs.nh3n as load_nh3n,
            rs.debit * rs.tss as load_tss
        from raw_acqs rs
        join spr_.logger ls
            on ls.id = rs.logger_id
    ),
    inserted as (
        insert into spr_.acq (
            logger_id, ts, n, ph, debit,
            cod, nh3n, tss,
            load_cod, load_nh3n, load_tss,
            load_total, errors
        )
            select rs.*,
                spr.errors(
                    ls,
                    rs.n, rs.ph, rs.debit,
                    rs.cod, rs.nh3n, rs.tss
                )::ltree[]
            from (
                select *,
                coalesce(load_cod, 0.0)
                    + coalesce(load_nh3n,0.0)
                    + coalesce(load_tss,0.0)
                as load_total
                from acqs
            ) rs
            join spr_.logger ls
                on ls.id = rs.logger_id
        on conflict (logger_id, ts)
        do update set
            n = excluded.n,
            ph = excluded.ph,
            debit = excluded.debit,

            cod = excluded.cod,
            nh3n = excluded.nh3n,
            tss = excluded.tss,

            load_cod = excluded.load_cod,
            load_nh3n = excluded.load_nh3n,
            load_tss = excluded.load_tss,
            load_total = excluded.load_total,

            errors = excluded.errors
        returning *
    )
    select count(1),
        count(distinct logger_id),
        min(ts),
        max(ts)
    into a.count, a.logger_count, a.log_min_ts, a.log_max_ts
    from inserted;
    return a;

end;
$$ language plpgsql;


create function spr_admin.web_acq_put (req jsonb)
returns jsonb
as $$
    select to_jsonb(spr_admin.web_acq_put(
        jsonb_populate_record(
            null::spr_admin.web_acq_put_it,
            spr_admin.auth(req))
    ))
$$ language sql stable;


\if :test
    create function tests.test_spr_admin_web_acq_put () returns setof text as $$
    declare
        sid jsonb = tests.session_as_admin();
        a jsonb;
        ts bigint = spr.trunc_ts(spr_.to_ts(current_timestamp));
    begin
        insert into spr_.log (ts, data) values
            (ts, '{"id":"dev1","ph":1}'::jsonb),
            (ts + 60, '{"id":"dev1","ph":2,"debit":100,"cod":10}'::jsonb);

        a = spr_admin.web_acq_put(sid);
        return next ok((a->>'count')::numeric = 1, 'took both logs');

        a = spr_admin.web_acq_get(sid);
        return next ok((a->'acqs'->'dev1'->0->>'ph')::numeric = 1.5, 'averaged the values');
        return next ok((a->'acqs'->'dev1'->0->>'n')::numeric = 2, 'came from 2 logs');
        return next ok((a->'acqs'->'dev1'->0->>'load_cod')::numeric = 1000, 'able to calc load_cod');
        return next ok((a->'acqs'->'dev1'->0->>'load_total')::numeric = 1000, 'able to calc load_total');
        return next ok(
            (select array_agg(b)::ltree[]
            from jsonb_array_elements_text(a->'acqs'->'dev1'->0->'errors') b
            ) ~ 'error.below_min_ph'
        , 'able to calc errors');
    end;
    $$ language plpgsql;
\endif
