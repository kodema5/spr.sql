create extension if not exists "uuid-ossp" schema public;
create extension if not exists pgcrypto schema public;
create extension if not exists ltree schema public;

\if :{?sparing_sql}
\else
\set sparing_sql true

\if :test
\if :local
    drop schema if exists sparing_ cascade;
\endif
\endif
create schema if not exists sparing_;
drop schema if exists sparing cascade;
create schema sparing;

\ir log.sql
\ir acq.sql
\ir get.sql

\endif
