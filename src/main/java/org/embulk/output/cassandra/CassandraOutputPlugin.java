package org.embulk.output.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.cassandra.setter.CassandraColumnSetter;
import org.embulk.output.cassandra.setter.CassandraColumnSetterFactory;
import org.embulk.output.cassandra.setter.ColumnSetterVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public class CassandraOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("hosts")
        public List<String> gethosts();

        @Config("port")
        @ConfigDefault("9042")
        public int getPort();

        @Config("username")
        @ConfigDefault("null")
        public Optional<String> getUsername();

        @Config("password")
        @ConfigDefault("null")
        public Optional<String> getPassword();

        @Config("cluster_name")
        @ConfigDefault("null")
        public Optional<String> getClustername();

        @Config("keyspace")
        public String getKeyspace();

        @Config("table")
        public String getTable();

        @Config("if_not_exists")
        @ConfigDefault("false")
        public Boolean getIfNotExists();

        @Config("ttl")
        @ConfigDefault("null")
        public Optional<Integer> getTtl();

        @Config("idempotent")
        @ConfigDefault("false")
        public Boolean getIdempotent();

        @Config("connect_timeout")
        @ConfigDefault("5000")
        public int getConnectTimeout();

        @Config("request_timeout")
        @ConfigDefault("12000")
        public int getRequestTimeout();
    }

    private final Logger logger = Exec.getLogger(getClass());

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // retryable (idempotent) output:
        // return resume(task.dump(), schema, taskCount, control);

        // non-retryable (non-idempotent) output:
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("cassandra output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        PageReader pageReader = new PageReader(schema);
        Cluster cluster = getCluster(task);

        Session session = cluster.newSession();

        TableMetadata tableMetadata = cluster.getMetadata()
                .getKeyspace(task.getKeyspace())
                .getTable(task.getTable());

        Insert insert = QueryBuilder.insertInto(task.getKeyspace(), task.getTable());
        if (task.getIfNotExists()) {
            insert.ifNotExists();
        }
        if (task.getTtl().isPresent()) {
            insert.using(QueryBuilder.ttl(task.getTtl().get()));
        }

        ImmutableMap.Builder<String, CassandraColumnSetter> columnSettersBuilder = ImmutableMap.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            insert.value(column.getName(), QueryBuilder.bindMarker(column.getName()));
            columnSettersBuilder.put(column.getName(), CassandraColumnSetterFactory.createColumnSetter(column));
        }
        Map<String, CassandraColumnSetter> columnSetters = columnSettersBuilder.build();
        List<ColumnSetterVisitor> columnVisitors = Lists.transform(schema.getColumns(), (column) ->
                new ColumnSetterVisitor(pageReader, columnSetters.get(column.getName())));

        logger.info("Insert Query: {}", insert.getQueryString());

        PreparedStatement prepared = session.prepare(insert);
        if (task.getIdempotent()) {
            prepared.setIdempotent(task.getIdempotent());
        }

        return new PluginPageOuput(cluster, session, pageReader, tableMetadata, columnSetters, columnVisitors, prepared, task);
    }

    private Cluster getCluster(PluginTask task)
    {
        Cluster.Builder builder = Cluster.builder();
        for (String host : task.gethosts()) {
            builder.addContactPointsWithPorts(new InetSocketAddress(host, task.getPort()));
        }

        if (task.getUsername().isPresent()) {
            builder.withCredentials(task.getUsername().get(), task.getPassword().orNull());
        }

        if (task.getClustername().isPresent()) {
            builder.withClusterName(task.getClustername().get());
        }

        builder.withSocketOptions(
                new SocketOptions()
                        .setConnectTimeoutMillis(task.getConnectTimeout())
                        .setReadTimeoutMillis(task.getRequestTimeout()));

        return builder.build();
    }

    public class PluginPageOuput implements TransactionalPageOutput
    {
        private final Cluster cluster;
        private final Session session;
        private final PageReader pageReader;
        private final TableMetadata tableMetadata;
        private final Map<String, CassandraColumnSetter> columnSetters;
        private final List<ColumnSetterVisitor> columnVisitors;
        private final PreparedStatement prepared;
        private final PluginTask task;

        public PluginPageOuput(
                Cluster cluster,
                Session session,
                PageReader pageReader,
                TableMetadata tableMetadata,
                Map<String, CassandraColumnSetter> columnSetters,
                List<ColumnSetterVisitor> columnVisitors,
                PreparedStatement prepared,
                PluginTask task)
        {
            this.cluster = cluster;
            this.session = session;
            this.pageReader = pageReader;
            this.tableMetadata = tableMetadata;
            this.columnSetters = columnSetters;
            this.columnVisitors = columnVisitors;
            this.prepared = prepared;
            this.task = task;
        }

        @Override
        public void add(Page page)
        {
            pageReader.setPage(page);
            BoundStatement statemtnt = prepared.bind();

            for (int i = 0; i < pageReader.getSchema().getColumns().size(); i++) {
                ColumnSetterVisitor visitor = columnVisitors.get(i);
                visitor.setStatement(statemtnt);
                pageReader.getSchema().getColumn(i).visit(visitor);
            }
            session.execute(statemtnt);
        }

        @Override
        public void finish()
        {

        }

        @Override
        public void close()
        {
            session.close();
            cluster.close();
        }

        @Override
        public void abort()
        {

        }

        @Override
        public TaskReport commit()
        {
            return null;
        }
    }
}
