create table if not exists spr_.logger (
    id text not null default md5(uuid_generate_v4()::text) primary key,

    name text not null,
    tags ltree[],
    is_enabled boolean default true,
    eval_method text default 'avg',

    has_ph boolean default false,
    min_ph float,
    max_ph float,

    has_debit boolean default false,
    max_debit float,

    has_cod boolean default false,
    max_cod float,
    max_load_cod float,

    has_nh3n boolean default false,
    max_nh3n float,
    max_load_nh3n float,

    has_tss boolean default false,
    max_tss float,
    max_load_tss float,

    max_load_total float
);
