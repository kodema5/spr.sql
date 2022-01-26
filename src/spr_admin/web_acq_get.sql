
create type spr_admin.web_acq_get_it as (
    _auth spr.auth_t,
    acq_min_ts bigint,
    acq_max_ts bigint
);


create function spr_admin.web_acq_get(req jsonb) returns jsonb as $$
declare
    it spr_admin.web_acq_get_it = jsonb_populate_record(null::spr_admin.web_acq_get_it, spr_admin.auth(req));
    a jsonb;
begin
    select coalesce(it.acq_min_ts, min(ts)), coalesce(it.acq_max_ts, max(ts))
    into it.acq_min_ts, it.acq_max_ts
    from only spr_.acq;


    return jsonb_build_object(
        'acqs', (
            select jsonb_object_agg(logger_id, rows)
            from (
                select ds.logger_id, jsonb_agg(to_jsonb(ds)) as rows
                from spr_.acq ds
                where ts between it.acq_min_ts and it.acq_max_ts
                group by ds.logger_id
            ) rs
        )
    );
end;
$$ language plpgsql;


\if :test
    create function tests.test_spr_admin_web_acq_get () returns setof text as $$
    declare
        sid jsonb = tests.session_as_admin();
        a jsonb;
        ts bigint = spr.trunc_ts(spr_.to_ts(current_timestamp));
    begin
        insert into spr_.acq (logger_id, ts, ph) values
            ('dev1', ts, 1.0);

        a = spr_admin.web_acq_get(sid);

        return next ok((a->'acqs'->'dev1'->0->>'ph')::numeric = 1, 'able to get data');
    end;
    $$ language plpgsql;
\endif