create type spr_admin.web_log_get_it as (
    _auth spr.auth_t,
    log_min_ts bigint,
    log_max_ts bigint
);


create function spr_admin.web_log_get (req jsonb) returns jsonb as $$
declare
    it spr_admin.web_log_get_it = jsonb_populate_record(null::spr_admin.web_log_get_it, spr_admin.auth(req));
begin
    select coalesce(it.log_min_ts, min(ts)), coalesce(it.log_max_ts, max(ts))
    into it.log_min_ts, it.log_max_ts
    from only spr_.log;

    return jsonb_build_object(
        'logs', (
            select jsonb_agg(to_jsonb(ds))
            from spr_.log ds
            where ts >= it.log_min_ts and ts <= it.log_max_ts
        )
    );
end;
$$ language plpgsql;


\if :test
    create function tests.test_web_log_get () returns setof text as $$
    declare
        sid jsonb = tests.session_as_admin();
        a jsonb;
    begin
        insert into spr_.log (data) values ('{}'::jsonb);
        a = spr_admin.web_log_get(sid);
        return next ok(jsonb_array_length(a->'logs') = 1, 'able to get log');
    end;
    $$ language plpgsql;
\endif