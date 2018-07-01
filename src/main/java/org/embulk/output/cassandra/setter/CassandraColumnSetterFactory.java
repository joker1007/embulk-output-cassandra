package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;

public class CassandraColumnSetterFactory
{
    private CassandraColumnSetterFactory()
    {
    }

    public static CassandraColumnSetter createColumnSetter(ColumnMetadata cassandraColumn, Cluster cluster)
    {
        switch (cassandraColumn.getType().getName()) {
            case TEXT:
            case VARCHAR:
            case ASCII:
                return new TextColumnSetter(cassandraColumn);
            case BOOLEAN:
                return new BooleanColumnSetter(cassandraColumn);
            case BIGINT:
                return new BigintColumnSetter(cassandraColumn);
            case INT:
                return new IntColumnSetter(cassandraColumn);
            case SMALLINT:
                return new SmallintColumnSetter(cassandraColumn);
            case TINYINT:
                return new TinyintColumnSetter(cassandraColumn);
            case VARINT:
                return new VarintColumnSetter(cassandraColumn);
            case DECIMAL:
                return new DecimalColumnSetter(cassandraColumn);
            case DOUBLE:
                return new DoubleColumnSetter(cassandraColumn);
            case FLOAT:
                return new FloatColumnSetter(cassandraColumn);
            case TIMESTAMP:
                return new TimestampColumnSetter(cassandraColumn);
            case DATE:
                return new DateColumnSetter(cassandraColumn);
            case TIME:
                return new TimeColumnSetter(cassandraColumn);
            case UUID:
                return new UuidColumnSetter(cassandraColumn);
            case TIMEUUID:
                return new TimeuuidColumnSetter(cassandraColumn);
            case LIST:
                return new ListColumnSetter(cassandraColumn);
            case MAP:
                return new MapColumnSetter(cassandraColumn);
            case SET:
                return new SetColumnSetter(cassandraColumn);
            case INET:
                return new InetColumnSetter(cassandraColumn);
            case TUPLE:
                return new TupleColumnSetter(cassandraColumn, cluster);
        }
        throw new RuntimeException(cassandraColumn.getName() + ":" + cassandraColumn.getType().getName() + " is unsupported Type");
    }
}
