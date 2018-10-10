package org.embulk.output.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.io.Resources;
import org.embulk.config.ConfigSource;
import org.embulk.spi.OutputPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestCassandraOutputPlugin
{
    private static final String RESOURCE_PATH = "org/embulk/output/cassandra/";

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(OutputPlugin.class, "cassandra", CassandraOutputPlugin.class)
            .build();

    private Cluster cluster;
    private Session session;

    private static String getCassandraHost()
    {
        String host = System.getenv("CASSANDRA_HOST");
        if (host == null) {
            host = "localhost";
        }
        return host;
    }

    private static List<String> getCassandraHostAsList()
    {
        return Arrays.asList(getCassandraHost());
    }

    private static String getCassandraPort()
    {
        String port = System.getenv("CASSANDRA_PORT");
        if (port == null) {
            port = "9042";
        }
        return port;
    }

    private static Cluster getCluster()
    {
        return Cluster.builder().addContactPoint(getCassandraHost())
                .withPort(Integer.parseInt(getCassandraPort())).build();
    }

    @Before
    public void setup()
    {
        cluster = getCluster();
        session = cluster.connect();
        String createKeyspace = EmbulkTests.readResource(RESOURCE_PATH + "create_keyspace.cql");
        String createTableBasic = EmbulkTests.readResource(RESOURCE_PATH + "create_table_test_basic.cql");
        String createTableUuid = EmbulkTests.readResource(RESOURCE_PATH + "create_table_test_uuid.cql");
        String createTableComplex = EmbulkTests.readResource(RESOURCE_PATH + "create_table_test_complex.cql");
        String createTableCounter = EmbulkTests.readResource(RESOURCE_PATH + "create_table_test_counter.cql");
        session.execute(createKeyspace);
        session.execute(createTableBasic);
        session.execute(createTableUuid);
        session.execute(createTableComplex);
        session.execute(createTableCounter);
        session.execute("TRUNCATE embulk_test.test_basic");
        session.execute("TRUNCATE embulk_test.test_uuid");
        session.execute("TRUNCATE embulk_test.test_complex");
        session.execute("TRUNCATE embulk_test.test_counter");
    }

    @After
    public void teardown()
    {
        session.close();
        cluster.close();
    }

    private ConfigSource loadYamlResource(String filename)
    {
        return embulk.loadYamlResource(RESOURCE_PATH + filename);
    }

    @Test
    public void testBasic() throws IOException
    {
        Path input = getInputPath("test1.csv");
        ConfigSource config = loadYamlResource("test_basic.yaml");
        config.set("hosts", getCassandraHostAsList());

        assertEquals(0, session.execute("SELECT * FROM embulk_test.test_basic").all().size());

        TestingEmbulk.RunResult result = embulk.runOutput(config, input);
        assertEquals(3, result.getOutputTaskReports().get(0).get(Long.class, "inserted_record_count").longValue());

        Row row1 = session.execute("SELECT * FROM embulk_test.test_basic WHERE id = 'A001'").one();
        Row row2 = session.execute("SELECT * FROM embulk_test.test_basic WHERE id = 'A002'").one();
        Row row3 = session.execute("SELECT * FROM embulk_test.test_basic WHERE id = 'A003'").one();
        assertEquals("A001", row1.getString("id"));
        assertEquals(9, row1.getLong("int_item"));
        assertEquals(1, row1.getInt("int32_item"));
        assertEquals(2, row1.getShort("smallint_item"));
        assertTrue(row1.getBool("boolean_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 0, 0), row1.getTimestamp("timestamp_item"));
        assertEquals("A002", row2.getString("id"));
        assertEquals(0, row2.getLong("int_item"));
        assertEquals(0, row2.getInt("int32_item"));
        assertEquals(4, row2.getShort("smallint_item"));
        assertTrue(row2.getBool("boolean_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 1, 0), row2.getTimestamp("timestamp_item"));
        assertEquals("A003", row3.getString("id"));
        assertEquals(9, row3.getLong("int_item"));
        assertEquals(0, row3.getInt("int32_item"));
        assertEquals(8, row3.getShort("smallint_item"));
        assertFalse(row3.getBool("boolean_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 2, 0), row3.getTimestamp("timestamp_item"));
    }

    @Test
    public void testCounter() throws IOException
    {
        Path input = getInputPath("test1.csv");
        ConfigSource config = loadYamlResource("test_counter.yaml");
        config.set("hosts", getCassandraHostAsList());

        assertEquals(0, session.execute("SELECT * FROM embulk_test.test_counter").all().size());

        TestingEmbulk.RunResult result = embulk.runOutput(config, input);
        assertEquals(3, result.getOutputTaskReports().get(0).get(Long.class, "inserted_record_count").longValue());

        List<Row> rows = session.execute("SELECT * FROM embulk_test.test_counter").all();
        rows.sort(Comparator.comparing(row -> row.getString("id")));
        Row row1 = rows.get(0);
        Row row2 = rows.get(1);
        Row row3 = rows.get(2);
        assertEquals("A001", row1.getString("id"));
        assertEquals(9, row1.getLong("int_item"));
        assertEquals(1, row1.getInt("int32_item"));
        assertEquals(2, row1.getShort("smallint_item"));
        assertTrue(row1.getBool("boolean_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 0, 0), row1.getTimestamp("timestamp_item"));
        assertEquals("A002", row2.getString("id"));
        assertEquals(0, row2.getLong("int_item"));
        assertEquals(0, row2.getInt("int32_item"));
        assertEquals(4, row2.getShort("smallint_item"));
        assertTrue(row2.getBool("boolean_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 1, 0), row2.getTimestamp("timestamp_item"));
        assertEquals("A003", row3.getString("id"));
        assertEquals(9, row3.getLong("int_item"));
        assertEquals(0, row3.getInt("int32_item"));
        assertEquals(8, row3.getShort("smallint_item"));
        assertFalse(row3.getBool("boolean_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 2, 0), row3.getTimestamp("timestamp_item"));
    }

    @Test
    public void testBasicWithTtl() throws IOException
    {
        Path input = getInputPath("test1.csv");
        ConfigSource config = loadYamlResource("test_basic.yaml");
        config.set("hosts", getCassandraHostAsList());
        config.set("ttl", 30);

        assertEquals(0, session.execute("SELECT * FROM embulk_test.test_basic").all().size());

        embulk.runOutput(config, input);

        Row row1 = session.execute("SELECT * FROM embulk_test.test_basic WHERE id = 'A001'").one();
        Row row2 = session.execute("SELECT * FROM embulk_test.test_basic WHERE id = 'A002'").one();
        Row row3 = session.execute("SELECT * FROM embulk_test.test_basic WHERE id = 'A003'").one();
        assertEquals("A001", row1.getString("id"));
        assertEquals(9, row1.getLong("int_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 0, 0), row1.getTimestamp("timestamp_item"));
        assertEquals("A002", row2.getString("id"));
        assertEquals(0, row2.getLong("int_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 1, 0), row2.getTimestamp("timestamp_item"));
        assertEquals("A003", row3.getString("id"));
        assertEquals(9, row3.getLong("int_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 2, 0), row3.getTimestamp("timestamp_item"));
    }

    @Test
    public void testBasicWithIfNotExists() throws IOException
    {
        Path input = getInputPath("test1.csv");
        ConfigSource config = loadYamlResource("test_basic.yaml");
        config.set("hosts", getCassandraHostAsList());
        config.set("if_not_exists", true);

        assertEquals(0, session.execute("SELECT * FROM embulk_test.test_basic").all().size());

        embulk.runOutput(config, input);

        Row row1 = session.execute("SELECT * FROM embulk_test.test_basic WHERE id = 'A001'").one();
        Row row2 = session.execute("SELECT * FROM embulk_test.test_basic WHERE id = 'A002'").one();
        Row row3 = session.execute("SELECT * FROM embulk_test.test_basic WHERE id = 'A003'").one();
        assertEquals("A001", row1.getString("id"));
        assertEquals(9, row1.getLong("int_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 0, 0), row1.getTimestamp("timestamp_item"));
        assertEquals("A002", row2.getString("id"));
        assertEquals(0, row2.getLong("int_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 1, 0), row2.getTimestamp("timestamp_item"));
        assertEquals("A003", row3.getString("id"));
        assertEquals(9, row3.getLong("int_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 2, 0), row3.getTimestamp("timestamp_item"));
    }

    @Test
    public void testUuid() throws IOException
    {
        Path input = getInputPath("test2.csv");
        ConfigSource config = loadYamlResource("test_uuid.yaml");
        config.set("hosts", getCassandraHostAsList());
        embulk.runOutput(config, input);

        Row row1 = session.execute("SELECT * FROM embulk_test.test_uuid").one();
        assertNotNull(row1.getUUID("id"));
        assertEquals(9, row1.getLong("int_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 0, 0), row1.getTimestamp("timestamp_item"));
        assertNotNull(row1.getUUID("timeuuid_item"));
        assertTrue(UUIDs.unixTimestamp(row1.getUUID("timeuuid_item")) < ZonedDateTime.now().toInstant().toEpochMilli());
    }

    @Test
    public void testBasicWithUpdateMode() throws IOException
    {
        Path input = getInputPath("test1.csv");
        ConfigSource config = loadYamlResource("test_basic.yaml");
        config.set("hosts", getCassandraHostAsList());
        config.set("mode", "update");

        assertEquals(0, session.execute("SELECT * FROM embulk_test.test_basic").all().size());

        embulk.runOutput(config, input);

        Row row1 = session.execute("SELECT * FROM embulk_test.test_basic WHERE id = 'A001'").one();
        Row row2 = session.execute("SELECT * FROM embulk_test.test_basic WHERE id = 'A002'").one();
        Row row3 = session.execute("SELECT * FROM embulk_test.test_basic WHERE id = 'A003'").one();
        assertEquals("A001", row1.getString("id"));
        assertEquals(9, row1.getLong("int_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 0, 0), row1.getTimestamp("timestamp_item"));
        assertEquals("A002", row2.getString("id"));
        assertEquals(0, row2.getLong("int_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 1, 0), row2.getTimestamp("timestamp_item"));
        assertEquals("A003", row3.getString("id"));
        assertEquals(9, row3.getLong("int_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 2, 0), row3.getTimestamp("timestamp_item"));
    }

    @Test
    public void testBasicWithIfExists() throws IOException
    {
        Path input = getInputPath("test1.csv");
        ConfigSource config = loadYamlResource("test_basic.yaml");
        config.set("hosts", getCassandraHostAsList());
        config.set("mode", "update");
        config.set("if_exists", true);

        session.execute("INSERT INTO embulk_test.test_basic (id) VALUES ('A001')");
        assertEquals(1, session.execute("SELECT * FROM embulk_test.test_basic").all().size());

        embulk.runOutput(config, input);

        Row row1 = session.execute("SELECT * FROM embulk_test.test_basic WHERE id = 'A001'").one();
        Row row2 = session.execute("SELECT * FROM embulk_test.test_basic WHERE id = 'A002'").one();
        Row row3 = session.execute("SELECT * FROM embulk_test.test_basic WHERE id = 'A003'").one();
        assertEquals("A001", row1.getString("id"));
        assertEquals(9, row1.getLong("int_item"));
        assertEquals(createDate(2018, 7, 1, 10, 0, 0, 0), row1.getTimestamp("timestamp_item"));
        assertNull(row2);
        assertNull(row3);
    }

    @Test
    public void testComplex() throws IOException
    {
        Path input = getInputPath("test3.csv");
        ConfigSource config = loadYamlResource("test_complex.yaml");
        config.set("hosts", getCassandraHostAsList());
        embulk.runOutput(config, input);

        Row row1 = session.execute("SELECT * FROM embulk_test.test_complex").one();
        assertNotNull(row1.getUUID("id"));
        assertEquals(BigDecimal.valueOf(10), row1.getDecimal("decimal_item"));
        assertEquals(LocalDate.fromYearMonthDay(2018, 7, 1), row1.getDate("date_item"));
        assertEquals(3600L * 10 * 1000 * 1000 * 1000, row1.getTime("time_item"));

        List<String> list = row1.getList("list_item", String.class);
        assertArrayEquals(new String[] {"foo", "bar"}, list.toArray());

        Map<String, Long> map = row1.getMap("map_item", String.class, Long.class);
        assertEquals(1L, map.get("key1").longValue());

        Set<Long> set = row1.getSet("set_item", Long.class);
        assertArrayEquals(new Long[] {1L, 2L, 3L}, set.toArray());

        InetAddress inet = row1.getInet("inet_item");
        assertEquals(InetAddress.getByName("127.0.0.1"), inet);

        TupleValue tuple = row1.getTupleValue("tuple_item");
        TupleType tupleType = cluster.getMetadata().newTupleType(DataType.text(), DataType.cdouble());
        assertEquals(tupleType.newValue("foo", 1.1), tuple);
    }

    private Path getInputPath(String filename)
    {
        return new File(Resources.getResource(RESOURCE_PATH + filename).getFile()).toPath();
    }

    private Date createDate(int year, int month, int day, int hour, int min, int sec, int nsec)
    {
        return Date.from(ZonedDateTime.of(year, month, day, hour, min, sec, nsec, ZoneId.of("UTC")).toInstant());
    }
}
