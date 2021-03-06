create type spr.auth_t as (
    is_admin boolean
);

-- override this as needed
--
create function spr.auth(
    req jsonb
)
    returns jsonb
    language plpgsql
as $$
declare
    a spr.auth_t;
begin
    -- override the following as needed
    a.is_admin = true;
    return req || jsonb_build_object('_auth', a);
end;
$$;