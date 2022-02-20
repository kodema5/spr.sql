create function spr_admin.has_table(
    tbl text,
    sch text default 'spr_')
returns boolean
as $$
    select exists(
        select 1 from pg_catalog.pg_class c
        join pg_catalog.pg_namespace n on n.oid = c.relnamespace
        where n.nspname = sch
        and c.relname = tbl
        and c.relkind = 'r'
    )
$$ language sql stable;
