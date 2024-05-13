CREATE TABLE IF NOT EXISTS embulk_test.test_integer_as_temporal (
  id text,
  timestamp_item timestamp,
  date_item date,
  time_item time,
  primary key (id)
) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy' };
