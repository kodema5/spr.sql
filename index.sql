------------------------------------------------------------------------------
-- q: what a

create extension if not exists "uuid-ossp" schema public;
create extension if not exists pgcrypto schema public;
create extension if not exists ltree schema public;

------------------------------------------------------------------------------
-- ddl section
\if :local
    drop schema if exists spr_ cascade;
\endif
create schema if not exists spr_;
\ir src/spr_/index.sql

------------------------------------------------------------------------------
-- user api  section

drop schema if exists spr cascade;
create schema spr;
\ir src/spr/index.sql

------------------------------------------------------------------------------
-- admin api section

drop schema if exists spr_admin cascade;
create schema spr_admin;
\ir src/spr_admin/index.sql
