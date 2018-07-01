package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;

public class IntColumnSetter extends CassandraColumnSetter
{
    public IntColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatement statement)
    {
        if (value) {
            statement.setInt(cassandraColumn.getName(), 1);
        }
        else {
            statement.setInt(cassandraColumn.getName(), 0);
        }
    }

    @Override
    public void setLongValue(Long value, BoundStatement statement)
    {
        statement.setInt(cassandraColumn.getName(), value.intValue());
    }

    @Override
    public void setDoubleValue(Double value, BoundStatement statement)
    {
        statement.setInt(cassandraColumn.getName(), value.intValue());
    }

    @Override
    public void setStringValue(String value, BoundStatement statement)
    {
        statement.setInt(cassandraColumn.getName(), Integer.parseInt(value));
    }
}
