CREATE KEYSPACE IF NOT EXISTS embulk_test
WITH REPLICATION = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 1
};