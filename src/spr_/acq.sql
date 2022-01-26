create table if not exists spr_.acq (
    logger_id text not null references spr_.logger(id) on delete cascade,
    ts bigint default spr_.to_ts(clock_timestamp()),
    primary key (logger_id, ts),

    processed_ts bigint default spr_.to_ts(clock_timestamp()),

    n int default 0,

    ph float check (ph>=0.0 and ph<=14.0),
    debit float, -- m3/h = 1000L/h

    cod float,   -- mg/L
    nh3n float,  -- mg/L
    tss float,   -- mg/L

    load_cod float,
    load_nh3n float,
    load_tss float,
    load_total float,

    errors ltree[]
);

