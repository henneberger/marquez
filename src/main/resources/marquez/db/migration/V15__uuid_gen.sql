create extension if not exists "uuid-ossp";
alter table sources alter column uuid set default uuid_generate_v4();
alter table namespaces alter column uuid set default uuid_generate_v4();
alter table owners alter column uuid set default uuid_generate_v4();
alter table namespace_ownerships alter column uuid set default uuid_generate_v4();