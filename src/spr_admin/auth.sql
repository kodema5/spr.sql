create function spr_admin.auth(
    req jsonb
)
    returns jsonb
    language plpgsql
    security definer
as $$
begin
    req = spr.auth(req);
    if req['_auth'] is null or not req['_auth']['is_admin']::boolean
    then
        raise exception 'error.insufficient_previledge';
    end if;

    return req;
end;
$$;

