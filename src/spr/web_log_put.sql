create type spr.web_log_put_it as (
    id text,
    ph float,
    debit float,
    cod float,
    nh3n float,
    tss float
);

create type spr.web_log_put_t as (
    ts bigint
);

create function spr.web_log_put (
    it spr.web_log_put_it
)
    returns spr.web_log_put_t
    language plpgsql
    security definer
as $$
declare
    a spr.web_log_put_t;
begin
    if it.id is null
    then
        raise exception 'error.unrecognized_device';
    end if;


    if not exists (
        select from spr_.logger
        where id=it.id )
    then
        raise exception 'error.unrecognized_device';
    end if;

    insert into spr_.log (data)
    values (to_jsonb(it))
    returning ts
    into a.ts;

    return a;
end;
$$;


create function spr.web_log_put (
    req jsonb
)
    returns jsonb
    language plpgsql
    security definer
as $$
begin
    return to_jsonb(spr.web_log_put(
        jsonb_populate_record(
            null::spr.web_log_put_it,
            req)
    ));
exception
    when invalid_parameter_value then
        raise exception 'error.unrecognized_format';
end;
$$;

\if :test
    create function tests.test_spr_web_log_put ()
        returns setof text
        language plpgsql
    as $$
    declare
        a jsonb;
    begin
        return next throws_ok(format(
            'select spr.web_log_put(%L::jsonb)',
            to_jsonb('xxxx'::text)
        ), 'error.unrecognized_format');

        return next throws_ok(format(
            'select spr.web_log_put(%L::jsonb)',
            jsonb_build_object()
        ), 'error.unrecognized_device');

        a = spr.web_log_put(jsonb_build_object('id', 'dev1'));
        return next ok(a->>'ts' is not null, 'able to put data');
    end;
    $$;
\endif