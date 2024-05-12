package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

public class TinyintColumnSetter extends CassandraColumnSetter
{
    public TinyintColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatementBuilder statement)
    {
        if (value) {
            statement.setByte(cassandraColumn.getName(), (byte) 1);
        }
        else {
            statement.setByte(cassandraColumn.getName(), (byte) 0);
        }
    }

    @Override
    public void setLongValue(Long value, BoundStatementBuilder statement)
    {
        statement.setByte(cassandraColumn.getName(), value.byteValue());
    }

    @Override
    public void setDoubleValue(Double value, BoundStatementBuilder statement)
    {
        statement.setByte(cassandraColumn.getName(), value.byteValue());
    }

    @Override
    public void setStringValue(String value, BoundStatementBuilder statement)
    {
        statement.setByte(cassandraColumn.getName(), Byte.parseByte(value));
    }
}
