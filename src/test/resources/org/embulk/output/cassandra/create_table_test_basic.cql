CREATE TABLE IF NOT EXISTS embulk_test.test_basic (
  id text,
  int_item bigint,
  int32_item int,
  smallint_item smallint,
  tinyint_item tinyint,
  boolean_item boolean,
  double_item double,
  timestamp_item timestamp,
  primary key (id)
) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy' };
