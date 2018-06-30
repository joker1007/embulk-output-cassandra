Embulk::JavaPlugin.register_output(
  "cassandra", "org.embulk.output.cassandra.CassandraOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
