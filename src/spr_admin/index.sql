\ir auth.sql
\ir has_table.sql

\ir web_log_get.sql
\ir web_log_delete.sql

\ir web_acq_get.sql
\ir web_acq_put.sql








-- create function spr_admin.log_partition_put () returns trigger as $$
-- declare
--     t text = spr_admin.new_log_partition(old.ts);
-- begin
--     execute format(
--         'insert into %I.%I select ( %I.%I %s ).* '
--         'on conflict do nothing',
--         'spr_', t
--         'spr_', 'log',
--         quote_literal(old)
--     );

--     return null;
-- exception
--     when others then
--         raise warning 'error: % %', sqlerrm, sqlstate;
--         return null;
-- end;
-- $$ language plpgsql volatile cost 100;

-- drop trigger if exists spr_admin_log_partition_put on spr_.log cascade;

-- create trigger spr_admin_log_partition_put after delete on spr_.log for each row execute function spr_admin.log_partition_put();







-- -- drop function spr_admin.put_processed_log_to_partition() cascade;

-- \if :test
--     create function tests.test_spr_put_processed_log_to_partition () returns setof text as $$
--     declare
--         l spr_.log;
--         t text;
--         b boolean;
--     begin
--         insert into spr_.log (data) values ('{}'::jsonb) returning * into l;
--         delete from spr_.log where id = l.id;
--         return next ok((select count(1)=1 from spr_.log where id=l.id), 'processed log is still kept');

--         t = 'log_' || to_char(to_timestamp(l.ts), 'YYYY_MM_DD');
--         return next has_table('spr_'::name,t::name);

--         execute format ('select count(1)=1 from only %s.%s where id=%L', 'spr_', t, l.id) into b;
--         return next ok(b, 'processed log is stored in partition');

--         execute format ('delete from only %s.%s where id=%L', 'spr_', t, l.id);
--         execute format ('select count(1)=1 from only %s.%s where id=%L', 'spr_', t, l.id) into b;
--         return next ok(not b, 'processed log is now actually removed from table');
--     end;
--     $$ language plpgsql;
-- \endif









-- \if :test
--     create function tests.test_spr_admin_web_acq_put () returns setof text as $$
--     declare
--         sid jsonb = tests.session_as_admin();
--         l spr_.log;
--         a jsonb;
--     begin
--         insert into spr_.log (data) values ('{}'::jsonb) returning * into l;
--         a = spr_admin.web_acq_put(sid);
--         raise warning '----%',a;
--         return next 'hello';
--     end;
--     -- declare
--     --     l spr_.log;
--     --     t text;
--     --     b boolean;
--     -- begin
--     --     insert into spr_.log (data) values ('{}'::jsonb) returning * into l;
--     --     delete from spr_.log where id = l.id;
--     --     return next ok((select count(1)=1 from spr_.log where id=l.id), 'processed log is still kept');

--     --     t = 'log_' || to_char(to_timestamp(l.ts), 'YYYY_MM_DD');
--     --     return next has_table('spr_'::name,t::name);

--     --     execute format ('select count(1)=1 from only %s.%s where id=%L', 'spr_', t, l.id) into b;
--     --     return next ok(b, 'processed log is stored in partition');

--     --     execute format ('delete from only %s.%s where id=%L', 'spr_', t, l.id);
--     --     execute format ('select count(1)=1 from only %s.%s where id=%L', 'spr_', t, l.id) into b;
--     --     return next ok(not b, 'processed log is now actually removed from table');
--     -- end;
--     $$ language plpgsql;
-- \endif



-- -- get devices
-- create type spr_admin.web_devices_get_it as (
--     _auth spr.auth_t
-- );

-- create function spr_admin.web_devices_get(req jsonb) returns jsonb as $$
-- declare
--     it spr_admin.web_devices_get_it = jsonb_populate_record(null::spr_admin.web_devices_get_it, spr_admin.auth(req));
-- begin

--     return jsonb_build_object('devices', jsonb_agg(to_jsonb(ds)))
--     from spr_.logger ds;
-- end;
-- $$ language plpgsql;

-- -- get devices statistics
-- create type spr_admin.web_devices_head_it as (
--     _auth spr.auth_t,
--     logger_id text
-- );

-- create function spr_admin.web_devices_head(req jsonb) returns jsonb as $$
-- declare
--     it spr_admin.web_devices_head_it = jsonb_populate_record(null::spr_admin.web_devices_head_it, spr_admin.auth(req));
-- begin
--     return jsonb_build_object('status', true);
-- end;
-- $$ language plpgsql;

-- -- creates or update a new device
-- create type spr_admin.web_devices_put_it as (
--     _auth spr.auth_t
-- );


-- create function spr_admin.web_devices_put(req jsonb) returns jsonb as $$
-- declare
--     it spr_admin.web_devices_put_it = jsonb_populate_record(null::spr_admin.web_devices_put_it, spr_admin.auth(req));
-- begin
--     return jsonb_build_object('status', true);
-- end;
-- $$ language plpgsql;

-- -- delete devices
-- create type spr_admin.web_devices_delete_it as (
--     _auth spr.auth_t
-- );

-- create function spr_admin.web_devices_delete(req jsonb) returns jsonb as $$
-- declare
--     it spr_admin.web_devices_delete_it = jsonb_populate_record(null::spr_admin.web_devices_delete_it, spr_admin.auth(req));
-- begin
--     return jsonb_build_object('status', true);
-- end;
-- $$ language plpgsql;


-- \if :test
--     create function tests.test_spr_admin_web_devices_get() returns setof text as $$
--     declare
--         a jsonb;
--     begin
--         a = spr_admin.web_devices_get(null);
--         -- raise warning '-----devices%', a;

--         return next ok(true, 'hello');
--     end;
--     $$ language plpgsql;

-- \endif


