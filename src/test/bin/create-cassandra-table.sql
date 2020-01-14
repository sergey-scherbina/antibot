create keyspace if not exists antibot with replication = {
    'class' : 'SimpleStrategy', 'replication_factor' : 1 } ;

create table if not exists antibot.events(
  "type" text,
  ip inet,
  event_time int,
  is_bot boolean,
  url text,
  time_uuid timeuuid,
  primary key ((ip, event_time), time_uuid)
);
