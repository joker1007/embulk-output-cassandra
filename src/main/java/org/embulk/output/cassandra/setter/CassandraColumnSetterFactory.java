package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.ColumnMetadata;

public class CassandraColumnSetterFactory
{
    private CassandraColumnSetterFactory()
    {
    }

    public static CassandraColumnSetter createColumnSetter(ColumnMetadata cassandraColumn)
    {
        switch (cassandraColumn.getType().getName()) {
            case TEXT:
            case VARCHAR:
                return new TextColumnSetter(cassandraColumn);
            case BIGINT:
                return new BigintColumnSetter(cassandraColumn);
            case DECIMAL:
                return new DecimalColumnSetter(cassandraColumn);
            case DOUBLE:
                return new DoubleColumnSetter(cassandraColumn);
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
        }
        throw new RuntimeException(cassandraColumn.getName() + ":" + cassandraColumn.getType().getName() + " is unsupported Type");
    }
}
