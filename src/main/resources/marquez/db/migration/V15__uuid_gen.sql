create extension if not exists "uuid-ossp";
alter table sources alter column uuid set default uuid_generate_v4();