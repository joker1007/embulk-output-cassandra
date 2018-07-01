package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;

public class TinyintColumnSetter extends CassandraColumnSetter
{
    public TinyintColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatement statement)
    {
        if (value) {
            statement.setByte(cassandraColumn.getName(), (byte) 1);
        }
        else {
            statement.setByte(cassandraColumn.getName(), (byte) 0);
        }
    }

    @Override
    public void setLongValue(Long value, BoundStatement statement)
    {
        statement.setByte(cassandraColumn.getName(), value.byteValue());
    }

    @Override
    public void setDoubleValue(Double value, BoundStatement statement)
    {
        statement.setByte(cassandraColumn.getName(), value.byteValue());
    }

    @Override
    public void setStringValue(String value, BoundStatement statement)
    {
        statement.setByte(cassandraColumn.getName(), Byte.parseByte(value));
    }
}
