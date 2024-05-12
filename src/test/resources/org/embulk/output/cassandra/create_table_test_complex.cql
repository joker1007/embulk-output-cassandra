CREATE TABLE IF NOT EXISTS embulk_test.test_complex (
  id uuid,
  decimal_item decimal,
  date_item date,
  time_item time,
  list_item list<text>,
  map_item map<text, bigint>,
  map_item2 map<bigint, frozen<list<timestamp>>>,
  map_item3 map<text, timeuuid>,
  set_item set<bigint>,
  inet_item inet,
  tuple_item tuple<text, double>,
  duration_item duration,
  primary key (id)
) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy' };
