create function spr_admin.new_log_partition(ts bigint) returns text as $$
declare
    d timestamp with time zone = date_trunc('day', to_timestamp(ts));
    t text = 'log_' || to_char(d, 'YYYYMMDD');
    min_ts bigint;
    max_ts bigint;
begin
    if spr_admin.has_table(t) then
        return t;
    end if;

    min_ts = (extract(epoch from d))::bigint;
    max_ts = min_ts + (24 * 60 * 60); -- one day

    execute format (
        'create table %I.%I ( '
            ' primary key (id), '
            ' check ( ts between %s::bigint and %s::bigint ) '
        ' ) '
        'inherits ( %I.%I )',
        'spr_', t,
        min_ts, max_ts,
        'spr_', 'log');
    return t;
end;
$$ language plpgsql;



-- deletes records from the log table
-- if min_ts and max_ts not provided, it is putting only data to partiion
--
create type spr_admin.web_log_delete_it as (
    _auth spr.auth_t,
    is_discard boolean,
    log_min_ts bigint,
    log_max_ts bigint
);

create function spr_admin.web_log_delete (req jsonb) returns jsonb as $$
declare
    it spr_admin.web_log_delete_it = jsonb_populate_record(null::spr_admin.web_log_delete_it, spr_admin.auth(req));
    rs spr_.log[];

begin

    it.is_discard = coalesce(it.is_discard, false);

    select coalesce(it.log_min_ts, min(ts)), coalesce(it.log_max_ts, max(ts))
    into it.log_min_ts, it.log_max_ts
    from only spr_.log;


    if not it.is_discard and (
        select count(1) from only spr_.log where ts between it.log_min_ts and it.log_max_ts
    ) <> (
        select count(1) from spr_.log where ts between it.log_min_ts and it.log_max_ts
    )
    then
        raise exception 'error.cross_archive_deletion_is_disallowed';
    end if;


    with
    deleted as (
        delete from spr_.log
        where ts between it.log_min_ts and it.log_max_ts
        returning *
    )
    select array_agg(ds) into rs from deleted ds;


    if not it.is_discard then
    declare
        r spr_.log;
        t text;
    begin
        foreach r in array rs loop
            t = spr_admin.new_log_partition(r.ts);
            execute format(
                'insert into %I.%I select ( %I.%I %s ).* '
                'on conflict do nothing',
                'spr_', t,
                'spr_', 'log',
                quote_literal(r)
            );
        end loop;
    end;
    end if;

    return jsonb_build_object(
        'count', count(1),
        'log_min_ts', min(ts),
        'log_max_ts', max(ts)
    )
    from unnest(rs);
end;
$$ language plpgsql;

\if :test
    create function tests.test_web_log_delete () returns setof text as $$
    declare
        sid jsonb = tests.session_as_admin();
        l spr_.log;
        a jsonb;
        t text;
        i int;
    begin
        insert into spr_.log (data) values ('{}'::jsonb) returning * into l;

        a = spr_admin.web_log_delete(sid);
        return next ok((a->>'count')::int = 1, 'removed from spr_.log');

        t = 'log_' || to_char(to_timestamp(l.ts), 'YYYYMMDD');
        return next has_table('spr_'::name, t::name);

        execute format('select count(1) from %s.%s','spr_', t) into i;
        return next ok(i = 1, 'actually stored into spr_.log_yyyymmdd');

        a = spr_admin.web_log_get(sid || jsonb_build_object(
            'log_min_ts', l.ts,
            'log_max_ts', l.ts
        ));
        return next ok(jsonb_array_length(a->'logs') = 1, 'log is stil there');

        return next throws_ok(format('select spr_admin.web_log_delete(%L::jsonb)', sid || jsonb_build_object(
            'log_min_ts', l.ts,
            'log_max_ts', l.ts
        )), 'error.cross_archive_deletion_is_disallowed');


        a = spr_admin.web_log_delete(sid || jsonb_build_object(
            'log_min_ts', l.ts,
            'log_max_ts', l.ts,
            'is_discard', true
        ));
        return next ok((a->>'count')::int = 1, 'really removed from spr_.log');

        a = spr_admin.web_log_get(sid || jsonb_build_object(
            'log_min_ts', l.ts,
            'log_max_ts', l.ts
        ));
        return next ok(jsonb_typeof(a->'logs') = 'null', 'really gone');
    end;
    $$ language plpgsql;
\endif

