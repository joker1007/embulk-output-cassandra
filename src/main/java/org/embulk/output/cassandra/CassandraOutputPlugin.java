package org.embulk.output.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.cassandra.setter.CassandraColumnSetter;
import org.embulk.output.cassandra.setter.CassandraColumnSetterFactory;
import org.embulk.output.cassandra.setter.ColumnSetterVisitor;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class CassandraOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("hosts")
        public List<String> getHosts();

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

        @Config("mode")
        @ConfigDefault("\"insert\"")
        public Mode getMode();

        @Config("if_not_exists")
        @ConfigDefault("false")
        public Boolean getIfNotExists();

        @Config("if_exists")
        @ConfigDefault("false")
        public Boolean getIfExists();

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

    private final Logger logger = Exec.getLogger(CassandraOutputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        control.run(taskSource);
        return Exec.newConfigDiff();
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

        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(task.getKeyspace());
        if (keyspaceMetadata == null) {
            throw new RuntimeException("keyspace `" + task.getKeyspace() + "` is not found");
        }

        TableMetadata tableMetadata = keyspaceMetadata.getTable(task.getTable());
        if (tableMetadata == null) {
            throw new RuntimeException("table `" + task.getTable() + "` is not found");
        }

        List<ColumnMetadata> columns = tableMetadata.getColumns();
        boolean isCounterTable = columns.stream().anyMatch(col -> col.getType().getName() == DataType.Name.COUNTER);

        BuiltStatement query;
        if (isCounterTable) {
            Update update = QueryBuilder.update(task.getKeyspace(), task.getTable());
            if (task.getTtl().isPresent()) {
                update.using(QueryBuilder.ttl(task.getTtl().get()));
            }
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                if (column.getType().getName() == DataType.Name.COUNTER) {
                    update.with(QueryBuilder.incr(column.getName(), QueryBuilder.bindMarker(column.getName())));
                }
                else {
                    update.where(QueryBuilder.eq(column.getName(), QueryBuilder.bindMarker(column.getName())));
                }
            }
            query = update;
        }
        else if (task.getMode() == Mode.UPDATE) {
            Update update = QueryBuilder.update(task.getKeyspace(), task.getTable());
            List<String> primaryKeys = tableMetadata.getPrimaryKey().stream().map(ColumnMetadata::getName).collect(Collectors.toList());
            List<String> columnNames = tableMetadata.getColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toList());

            if (task.getIfExists()) {
                update.where().ifExists();
            }
            if (task.getTtl().isPresent()) {
                update.using(QueryBuilder.ttl(task.getTtl().get()));
            }
            for (String pkey : primaryKeys) {
                update.where(QueryBuilder.eq(pkey, QueryBuilder.bindMarker(pkey)));
            }
            for (Column col : schema.getColumns()) {
                if (primaryKeys.contains(col.getName())) {
                    continue;
                }
                if (columnNames.contains(col.getName())) {
                    update.with(QueryBuilder.set(col.getName(), QueryBuilder.bindMarker(col.getName())));
                }
            }
            query = update;
        }
        else {
            Insert insert = QueryBuilder.insertInto(task.getKeyspace(), task.getTable());
            if (task.getIfNotExists()) {
                insert.ifNotExists();
            }
            if (task.getTtl().isPresent()) {
                insert.using(QueryBuilder.ttl(task.getTtl().get()));
            }
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                insert.value(column.getName(), QueryBuilder.bindMarker(column.getName()));
            }
            query = insert;
        }

        ImmutableMap.Builder<String, CassandraColumnSetter> columnSettersBuilder = ImmutableMap.builder();
        ImmutableList.Builder<String> uuidColumnsBuilder = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnSettersBuilder.put(column.getName(), CassandraColumnSetterFactory.createColumnSetter(column, cluster));
            switch (column.getType().getName()) {
                case UUID:
                case TIMEUUID:
                    uuidColumnsBuilder.add(column.getName());
            }
        }
        Map<String, CassandraColumnSetter> columnSetters = columnSettersBuilder.build();
        List<String> uuidColumns = uuidColumnsBuilder.build();
        List<ColumnSetterVisitor> columnVisitors = Lists.transform(schema.getColumns(), (column) ->
                new ColumnSetterVisitor(pageReader, columnSetters.get(column.getName())));

        logger.info("Query: {}", query.getQueryString());

        PreparedStatement prepared = session.prepare(query);
        if (task.getIdempotent()) {
            prepared.setIdempotent(task.getIdempotent());
        }

        return new PluginPageOuput(cluster, session, pageReader, tableMetadata, uuidColumns, columnSetters, columnVisitors, prepared, task);
    }

    private Cluster getCluster(PluginTask task)
    {
        Cluster.Builder builder = Cluster.builder();
        for (String host : task.getHosts()) {
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
        private final List<String> uuidColumns;
        private final Map<String, CassandraColumnSetter> columnSetters;
        private final List<ColumnSetterVisitor> columnVisitors;
        private final PreparedStatement prepared;
        private final PluginTask task;
        private long counter = 0;
        private long nextLoggingCount = 1;

        public PluginPageOuput(
                Cluster cluster,
                Session session,
                PageReader pageReader,
                TableMetadata tableMetadata,
                List<String> uuidColumns,
                Map<String, CassandraColumnSetter> columnSetters,
                List<ColumnSetterVisitor> columnVisitors,
                PreparedStatement prepared,
                PluginTask task)
        {
            this.cluster = cluster;
            this.session = session;
            this.pageReader = pageReader;
            this.tableMetadata = tableMetadata;
            this.uuidColumns = uuidColumns;
            this.columnSetters = columnSetters;
            this.columnVisitors = columnVisitors;
            this.prepared = prepared;
            this.task = task;
        }

        @Override
        public void add(Page page)
        {
            pageReader.setPage(page);
            while (pageReader.nextRecord()) {
                BoundStatement statement = prepared.bind();

                for (String uuidColumn : uuidColumns) {
                    columnSetters.get(uuidColumn).setNullValue(statement);
                }

                for (int i = 0; i < pageReader.getSchema().getColumns().size(); i++) {
                    ColumnSetterVisitor visitor = columnVisitors.get(i);
                    visitor.setStatement(statement);
                    if (visitor.hasSetter()) {
                        pageReader.getSchema().getColumn(i).visit(visitor);
                    }
                }
                session.execute(statement);
                counter++;
                if (counter >= nextLoggingCount) {
                    logger.info("Inserted {} records", counter);
                    nextLoggingCount = nextLoggingCount * 2;
                }
            }
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
            TaskReport report = Exec.newTaskReport();
            report.set("inserted_record_count", counter);
            return report;
        }
    }

    public enum Mode {
        INSERT,
        UPDATE;

        @JsonValue
        @Override
        public String toString()
        {
            return name().toLowerCase(Locale.ENGLISH);
        }

        @JsonCreator
        public static Mode fromString(String value)
        {
            switch(value) {
                case "insert":
                    return INSERT;
                case "update":
                    return UPDATE;
                default:
                    throw new ConfigException(String.format("Unknown mode '%s'", value));
            }
        }
    }
}
