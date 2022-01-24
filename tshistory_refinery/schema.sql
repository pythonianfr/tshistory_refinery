-- cache policy

create table "{ns}".cache_policy (
  id serial primary key,
  name text unique not null,

  -- two moment expressions
  from_date text not null,
  to_date text not null,

  -- two cron expressions
  revdate_rule text not null,
  schedule_rule text not null,

  unique (from_date, to_date, revdate_rule, schedule_rule)
);


create index on "{ns}".cache_policy (from_date);
create index on "{ns}".cache_policy (to_date);
create index on "{ns}".cache_policy (revdate_rule);
create index on "{ns}".cache_policy (schedule_rule);


create table "{ns}".cache_policy_series (
  cache_policy_id int not null references "{ns}".cache_policy on delete cascade,
  series_id int not null references "{ns}".formula on delete cascade,

  unique (cache_policy_id, series_id)
);

create index on "{ns}".cache_policy_series (cache_policy_id);
create index on "{ns}".cache_policy_series (series_id);
