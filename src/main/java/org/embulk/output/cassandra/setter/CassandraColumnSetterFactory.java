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
        }
        throw new RuntimeException(cassandraColumn.getName() + ":" + cassandraColumn.getType().getName() + " is unsupported Type");
    }
}
