create extension if not exists "uuid-ossp";
alter table sources alter column uuid set default uuid_generate_v4();
alter table namespaces alter column uuid set default uuid_generate_v4();
alter table owners alter column uuid set default uuid_generate_v4();
alter table namespace_ownerships alter column uuid set default uuid_generate_v4();
alter table tags alter column uuid set default uuid_generate_v4();
alter table runs alter column uuid set default uuid_generate_v4();
alter table run_states alter column uuid set default uuid_generate_v4();
alter table run_args alter column uuid set default uuid_generate_v4();