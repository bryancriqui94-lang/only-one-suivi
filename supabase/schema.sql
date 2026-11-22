-- Only One Suivi : base de données Supabase
-- Une seule table « docs » qui reprend les collections de l'appli (clients, prestas, prospects_communes…)

create table if not exists public.docs (
  col text not null,
  id text not null,
  data jsonb not null default '{}'::jsonb,
  version integer not null default 1,
  updated_at timestamptz not null default now(),
  primary key (col, id)
);

create index if not exists docs_col_idx on public.docs (col);
create index if not exists docs_statut_idx on public.docs (col, (data->>'statut'));

-- Version et date de mise à jour automatiques
create or replace function public.docs_touch() returns trigger
language plpgsql as $$
begin
  if tg_op = 'UPDATE' then
    new.version := old.version + 1;
  end if;
  new.updated_at := now();
  return new;
end $$;

drop trigger if exists docs_touch on public.docs;
create trigger docs_touch before insert or update on public.docs
for each row execute function public.docs_touch();

-- Fusion d'un document (équivalent de « update ») : les champs donnés remplacent les anciens,
-- un champ {"__delete__": true} est supprimé
create or replace function public.docs_merge(p_col text, p_id text, p_patch jsonb)
returns void language plpgsql security invoker as $$
declare
  dels text[];
  clean jsonb;
begin
  select coalesce(array_agg(key), '{}') into dels
    from jsonb_each(p_patch) where jsonb_typeof(value) = 'object' and value ? '__delete__';
  clean := p_patch - dels;
  insert into public.docs (col, id, data) values (p_col, p_id, clean)
  on conflict (col, id) do update set data = (public.docs.data || excluded.data) - dels;
end $$;

-- Sécurité : seul le compte de Bryan peut lire et écrire
alter table public.docs enable row level security;

drop policy if exists docs_owner on public.docs;
create policy docs_owner on public.docs
  for all to authenticated
  using ((auth.jwt() ->> 'email') = 'bryancriquipro@gmail.com')
  with check ((auth.jwt() ->> 'email') = 'bryancriquipro@gmail.com');

-- Temps réel (l'appli se met à jour seule quand l'agent écrit)
alter table public.docs replica identity full;
do $$ begin
  alter publication supabase_realtime add table public.docs;
exception when duplicate_object then null; end $$;
