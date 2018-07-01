CREATE TABLE IF NOT EXISTS embulk_test.test_uuid (
  id uuid,
  int_item bigint,
  double_item double,
  timestamp_item timestamp,
  timeuuid_item timeuuid,
  primary key (id)
) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy' };
