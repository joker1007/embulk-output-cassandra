CREATE TABLE IF NOT EXISTS embulk_test.test_complex (
  id uuid,
  decimal_item decimal,
  date_item date,
  time_item time,
  list_item list<text>,
  map_item map<text, bigint>,
  set_item set<bigint>,
  inet_item inet,
  tuple_item tuple<text, double>,
  primary key (id)
) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy' };
