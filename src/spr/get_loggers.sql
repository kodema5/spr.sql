create type spr.get_loggers_it as (
    logger_ids text[],
    logger_name text,
    logger_tags text[]
);

create function spr.is_empty(it spr.get_loggers_it) returns boolean as $$
    select case
        when it is null then true
        else (
            (it.logger_ids is null or cardinality(it.logger_ids)=0)
            and (it.logger_name is null or trim(it.logger_name)='')
            and (it.logger_tags is null or cardinality(it.logger_tags)=0)
        )
        end;
$$ language sql stable;


create function spr.get_loggers(it spr.get_loggers_it) returns setof spr_.logger as $$
    select ds
    from spr_.logger ds,
        (select unnest(coalesce(it.logger_tags, array['*']))) as ts (t)
    where (it.logger_ids is null or id=any(it.logger_ids))
    and (it.logger_name is null or name ~* it.logger_name)
    and tags ~ (ts.t::lquery)
$$ language sql stable;


create function spr.get_loggers(
    req jsonb,
    is_required boolean default true
) returns setof spr_.logger as $$
declare
    it spr.get_loggers_it = jsonb_populate_record(null::spr.get_loggers_it, req);
begin
    if is_required=true and spr.is_empty(it) then
        raise exception 'error.empty_get_loggers_parameters';
    end if;

    return query select * from spr.get_loggers(it);
end;
$$ language plpgsql;


\if :test
    create function tests.test_spr_get_loggers() returns setof text as $$
    declare
        a jsonb;
        ls spr_.logger[];
    begin

        return next ok(spr.is_empty(null::spr.get_loggers_it), 'check empty params');

        return next ok(spr.is_empty(jsonb_populate_record(
            null::spr.get_loggers_it,
            jsonb_build_object()
        )), 'check empty params');

        return next ok(spr.is_empty(jsonb_populate_record(
            null::spr.get_loggers_it,
            jsonb_build_object(
                'logger_ids', jsonb_build_array(),
                'logger_name', '',
                'logger_tags', jsonb_build_array()
            )
        )), 'check empty params');


        return next throws_ok('select spr.get_loggers(null::jsonb)',
            'error.empty_get_loggers_parameters');

        select array_agg(l) into ls from spr.get_loggers(jsonb_build_object(
            'logger_name', 'dev*'
        )) l;
        return next ok(ls[1].id= 'dev1', 'got a device by name');

        select array_agg(l) into ls from spr.get_loggers(jsonb_build_object(
            'logger_tags', jsonb_build_array('dev.*')
        )) l;
        return next ok(ls[1].id= 'dev1', 'got a device by tag');

        select array_agg(l) into ls from spr.get_loggers(jsonb_build_object(
            'logger_ids', jsonb_build_array('dev1')
        )) l;
        return next ok(ls[1].id= 'dev1', 'got a device by logger-id');

        select array_agg(l) into ls from spr.get_loggers(jsonb_build_object(
            'logger_ids', jsonb_build_array('dev2')
        )) l;
        return next ok(ls is null, 'no logger by id');

    end;
    $$ language plpgsql;
\endif