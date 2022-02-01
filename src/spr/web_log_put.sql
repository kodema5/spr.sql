create function spr.web_log_put(req jsonb) returns jsonb as $$
declare
    a bigint;
begin
    if jsonb_typeof(req) <> 'object' then
        raise exception 'error.unrecognized_format';
    end if;

    select count(1) into a  from spr_.logger where id=req->>'id';
    if a=0 then
        raise exception 'error.unrecognized_device';
    end if;

    insert into spr_.log (data) values (req) returning ts into a;
    return jsonb_build_object('ts', a);

end;
$$ language plpgsql;


\if :test
    create function tests.test_spr_web_log_put() returns setof text as $$
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
    $$ language plpgsql;
\endif