package org.embulk.output.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
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
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class CassandraOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("hosts")
        List<String> getHosts();

        @Config("port")
        @ConfigDefault("9042")
        int getPort();

        @Config("username")
        @ConfigDefault("null")
        Optional<String> getUsername();

        @Config("password")
        @ConfigDefault("null")
        Optional<String> getPassword();

        @Config("local_datacenter_name")
        @ConfigDefault("null")
        Optional<String> getLocalDatacenterName();

        @Config("local_datacenter_name")
        @ConfigDefault("\"DcInferringLoadBalancingPolicy\"")
        String getLoadBalancingPolicy();

        @Config("keyspace")
        String getKeyspace();

        @Config("table")
        String getTable();

        @Config("mode")
        @ConfigDefault("\"insert\"")
        Mode getMode();

        @Config("if_not_exists")
        @ConfigDefault("false")
        boolean getIfNotExists();

        @Config("if_exists")
        @ConfigDefault("false")
        boolean getIfExists();

        @Config("ttl")
        @ConfigDefault("null")
        Optional<Integer> getTtl();

        @Config("idempotent")
        @ConfigDefault("false")
        boolean getIdempotent();

        @Config("connect_timeout")
        @ConfigDefault("5000")
        int getConnectTimeout();

        @Config("request_timeout")
        @ConfigDefault("12000")
        int getRequestTimeout();
    }

    private static final Logger logger = LoggerFactory.getLogger(CassandraOutputPlugin.class);

    private final ConfigMapperFactory configMapperFactory = ConfigMapperFactory.withDefault();

    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  Schema schema, int taskCount,
                                  Control control)
    {
        ConfigMapper configMapper = configMapperFactory.createConfigMapper();
        PluginTask task = configMapper.map(config, PluginTask.class);

        return resume(task.toTaskSource(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             Schema schema, int taskCount,
                             Control control)
    {
        control.run(taskSource);
        return configMapperFactory.newConfigDiff();
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
        TaskMapper taskMapper = configMapperFactory.createTaskMapper();
        PluginTask task = taskMapper.map(taskSource, PluginTask.class);
        PageReader pageReader = Exec.getPageReader(schema);

        CqlSession session = getSession(task);

        KeyspaceMetadata keyspaceMetadata = session.getMetadata().getKeyspace(task.getKeyspace()).orElseThrow(() ->
                new ConfigException("keyspace `" + task.getKeyspace() + "` is not found"));

        TableMetadata tableMetadata = keyspaceMetadata.getTable(task.getTable()).orElseThrow(() ->
                new ConfigException("table `" + task.getTable() + "` is not found"));

        Map<CqlIdentifier, ColumnMetadata> columns = tableMetadata.getColumns();
        List<String> primaryKeys = tableMetadata.getPrimaryKey().stream().map(metadata -> metadata.getName().asInternal()).collect(Collectors.toList());
        Optional<CqlIdentifier> counterColumn = columns.entrySet().stream()
                .filter(e -> e.getValue().getType().equals(DataTypes.COUNTER))
                .map(Map.Entry::getKey)
                .findFirst();

        SimpleStatement query;
        if (counterColumn.isPresent()) {
            if (schema.getColumns().stream().noneMatch(col -> counterColumn.get().asInternal().equals(col.getName()))) {
                throw new ConfigException("counter column `" + counterColumn.get().asInternal() + "` is not found in schema");
            }

            UpdateStart updateStart = QueryBuilder.update(task.getKeyspace(), task.getTable());
            if (task.getTtl().isPresent()) {
                updateStart = updateStart.usingTtl(task.getTtl().get());
            }

            UpdateWithAssignments assignments = updateStart.increment(counterColumn.get(), QueryBuilder.bindMarker(counterColumn.get()));

            List<Relation> relations = schema.getColumns().stream()
                    .filter(col -> !col.getName().equals(counterColumn.get().asInternal()))
                    .filter(col -> columns.containsKey(CqlIdentifier.fromInternal(col.getName())))
                    .map(col -> Relation.column(col.getName()).isEqualTo(QueryBuilder.bindMarker(col.getName())))
                    .collect(Collectors.toList());
            Update update = assignments.where(relations);
            query = update.build();
        } else if (task.getMode() == Mode.UPDATE) {
            UpdateStart updateStart = QueryBuilder.update(task.getKeyspace(), task.getTable());
            if (task.getTtl().isPresent()) {
                updateStart = updateStart.usingTtl(task.getTtl().get());
            }

            List<Assignment> assignments = schema.getColumns().stream()
                    .filter(col -> !primaryKeys.contains(col.getName()))
                    .filter(col -> columns.containsKey(CqlIdentifier.fromInternal(col.getName())))
                    .map(col -> Assignment.setColumn(col.getName(), QueryBuilder.bindMarker(col.getName())))
                    .collect(Collectors.toList());
            UpdateWithAssignments updateWithAssignments = updateStart.set(assignments);

            List<Relation> relations = primaryKeys.stream()
                    .map(pkey -> Relation.column(pkey).isEqualTo(QueryBuilder.bindMarker(pkey)))
                    .collect(Collectors.toList());
            Update update = updateWithAssignments.where(relations);
            if (task.getIfExists()) {
                update = update.ifExists();
            }
            query = update.build();
        } else if (task.getMode() == Mode.DELETE) {
            DeleteSelection deleteSelection = QueryBuilder.deleteFrom(task.getKeyspace(), task.getTable());
            List<Relation> relations = primaryKeys.stream()
                    .map(pkey -> Relation.column(pkey).isEqualTo(QueryBuilder.bindMarker(pkey)))
                    .collect(Collectors.toList());
            Delete delete = deleteSelection.where(relations);
            query = delete.build();
        } else {
            InsertInto insertInto = QueryBuilder.insertInto(task.getKeyspace(), task.getTable());
            Map<CqlIdentifier, Term> assignments = schema.getColumns().stream()
                    .filter(col -> columns.containsKey(CqlIdentifier.fromInternal(col.getName())))
                    .collect(Collectors.toMap(
                            col -> CqlIdentifier.fromInternal(col.getName()),
                            col -> QueryBuilder.bindMarker(col.getName())));
            columns.values().stream()
                    .filter(col -> col.getType().equals(DataTypes.UUID) || col.getType().equals(DataTypes.TIMEUUID))
                    .forEach(col -> assignments.put(col.getName(), QueryBuilder.bindMarker()));
            Insert insert = insertInto.valuesByIds(assignments);
            if (task.getIfNotExists()) {
                insert = insert.ifNotExists();
            }
            if (task.getTtl().isPresent()) {
                insert = insert.usingTtl(task.getTtl().get());
            }
            query = insert.build();
        }

        ImmutableMap.Builder<String, CassandraColumnSetter> columnSettersBuilder = ImmutableMap.builder();
        ImmutableList.Builder<String> uuidColumnsBuilder = ImmutableList.builder();
        for (Map.Entry<CqlIdentifier, ColumnMetadata> column : columns.entrySet()) {
            columnSettersBuilder.put(column.getKey().asInternal(), CassandraColumnSetterFactory.createColumnSetter(column.getValue()));
            DataType type = column.getValue().getType();
            if (type.equals(DataTypes.UUID) || type.equals(DataTypes.TIMEUUID)) {
                uuidColumnsBuilder.add(column.getKey().asInternal());
            }
        }
        List<String> uuidColumns = uuidColumnsBuilder.build();

        Map<String, CassandraColumnSetter> columnSetters = columnSettersBuilder.build();
        List<ColumnSetterVisitor> columnVisitors = schema.getColumns().stream().map(column ->
                        new ColumnSetterVisitor(
                                pageReader,
                                columnSetters.get(column.getName()),
                                primaryKeys.contains(column.getName()),
                                task.getMode() == Mode.DELETE))
                .collect(Collectors.toList());

        logger.info("Query: {}", query.getQuery());

        if (task.getIdempotent() || task.getMode() == Mode.DELETE) {
            query = query.setIdempotent(true);
        }

        PreparedStatement prepared = session.prepare(query);

        return new PluginPageOuput(session, pageReader, uuidColumns, columnSetters, columnVisitors, prepared, task);
    }

    private CqlSession getSession(PluginTask task)
    {
        CqlSessionBuilder builder = CqlSession.builder();
        for (String host : task.getHosts()) {
            builder.addContactPoint(new InetSocketAddress(host, task.getPort()));
        }

        task.getUsername().ifPresent(username -> builder.withAuthCredentials(username, task.getPassword().orElse(null)));

        task.getLocalDatacenterName().ifPresent(builder::withLocalDatacenter);

        DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                .withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, task.getLoadBalancingPolicy())
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(task.getRequestTimeout()))
                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofMillis(task.getConnectTimeout()))
                .build();

        builder.withConfigLoader(loader);

        return builder.build();
    }

    public class PluginPageOuput implements TransactionalPageOutput
    {
        private final CqlSession session;
        private final PageReader pageReader;
        private final List<String> uuidColumns;
        private final Map<String, CassandraColumnSetter> columnSetters;
        private final List<ColumnSetterVisitor> columnVisitors;
        private final PreparedStatement prepared;
        private final PluginTask task;
        private long counter = 0;
        private long nextLoggingCount = 1;

        public PluginPageOuput(
                CqlSession session,
                PageReader pageReader,
                List<String> uuidColumns,
                Map<String, CassandraColumnSetter> columnSetters,
                List<ColumnSetterVisitor> columnVisitors,
                PreparedStatement prepared,
                PluginTask task)
        {
            this.session = session;
            this.pageReader = pageReader;
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
                BoundStatementBuilder statementBuilder = prepared.boundStatementBuilder();

                for (String uuidColumn : uuidColumns) {
                    columnSetters.get(uuidColumn).setNullValue(statementBuilder);
                }

                for (int i = 0; i < pageReader.getSchema().getColumns().size(); i++) {
                    ColumnSetterVisitor visitor = columnVisitors.get(i);
                    visitor.setStatementBuilder(statementBuilder);
                    if (visitor.hasSetter()) {
                        pageReader.getSchema().getColumn(i).visit(visitor);
                    }
                }
                session.execute(statementBuilder.build());
                counter++;
                if (counter >= nextLoggingCount) {
                    logger.info(task.getMode().logMessage(), counter);
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
        }

        @Override
        public void abort()
        {
        }

        @Override
        public TaskReport commit()
        {
            TaskReport report = configMapperFactory.newTaskReport();
            report.set("inserted_record_count", counter);
            return report;
        }
    }

    public enum Mode
    {
        INSERT {
            @Override
            public String logMessage()
            {
                return "Inserted {} records";
            }
        },
        UPDATE {
            @Override
            public String logMessage()
            {
                return "Updated {} records";
            }
        },
        DELETE {
            @Override
            public String logMessage()
            {
                return "Deleted {} records";
            }
        };

        @JsonValue
        @Override
        public String toString()
        {
            return name().toLowerCase(Locale.ENGLISH);
        }

        @JsonCreator
        public static Mode fromString(String value)
        {
            switch (value) {
                case "insert":
                    return INSERT;
                case "update":
                    return UPDATE;
                case "delete":
                    return DELETE;
                default:
                    throw new ConfigException(String.format("Unknown mode '%s'", value));
            }
        }

        public abstract String logMessage();
    }
}
