create keyspace if not exists antibot with replication = {
    'class' : 'SimpleStrategy', 'replication_factor' : 1 } ;

create table if not exists antibot.events(
  "type" text,
  ip inet,
  is_bot boolean,
  event_time timestamp,
  url text,
  primary key (ip, event_time, url)
);
