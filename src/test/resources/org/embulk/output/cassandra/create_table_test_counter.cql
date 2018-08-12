CREATE TABLE IF NOT EXISTS embulk_test.test_counter (
  id text,
  int_item counter,
  int32_item int,
  smallint_item smallint,
  tinyint_item tinyint,
  boolean_item boolean,
  double_item double,
  timestamp_item timestamp,
  primary key (id, int32_item, smallint_item, tinyint_item, boolean_item, double_item, timestamp_item)
) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy' };
