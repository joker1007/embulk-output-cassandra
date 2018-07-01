CREATE TABLE IF NOT EXISTS embulk_test.test_basic (
  id text,
  int_item bigint,
  double_item double,
  timestamp_item timestamp,
  primary key (id)
) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy' };
