-- cache policy

create table "{ns}".cache_policy (
  id serial primary key,
  name text unique not null,
  ready bool not null default false,

  -- four moment expressions
  initial_revdate text not null,
  look_before text not null,
  look_after text not null,

  -- two cron expressions
  revdate_rule text not null,
  schedule_rule text not null
);

create index on "{ns}".cache_policy (name);


create table "{ns}".cache_policy_sched (
  cache_policy_id int unique not null references "{ns}".cache_policy on delete cascade,
  prepared_task_id int not null references "rework".sched on delete cascade
);

create index on "{ns}".cache_policy_sched (cache_policy_id);
create index on "{ns}".cache_policy_sched (prepared_task_id);


create table "{ns}".cache_policy_series (
  cache_policy_id int not null references "{ns}".cache_policy on delete cascade,
  series_id int unique not null references "{ns}".registry on delete cascade,
  ready bool not null default true,

  unique (cache_policy_id, series_id)
);

create index on "{ns}".cache_policy_series (cache_policy_id);
create index on "{ns}".cache_policy_series (series_id);
