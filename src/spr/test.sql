\if :test
    create function tests.startup_spr ()
        returns setof text
        language plpgsql
    as $$
    begin
        insert into spr_.logger (id, name, tags, has_ph, min_ph, max_ph)
        values ('dev1', 'device-1', array['dev.logger.1']::ltree[], true, 5, 9);


        -- insert into spr_.acq(logger_id, debit, cod)
        -- values ('dev1', 1, 20);

        return next 'startup-spr';
    end;
    $$;

    create function tests.shutdown_spr ()
        returns setof text
        language plpgsql
    as $$
    begin
        delete from spr_.logger where id='dev-1';

        return next 'shutdown-spr';
    end;
    $$;

    create function tests.session_as_admin ()
        returns jsonb
        language plpgsql
    as $$
    declare
        a jsonb;
    begin
        a = jsonb_build_object(
            'session_id', 'foo', 'is_admin', true
        );
        return jsonb_build_object('session_id', a);
    end;
    $$;
\endif