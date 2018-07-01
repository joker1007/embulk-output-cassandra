package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;

public class FloatColumnSetter extends CassandraColumnSetter
{
    public FloatColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatement statement)
    {
        if (value) {
            statement.setFloat(cassandraColumn.getName(), 1);
        }
        else {
            statement.setFloat(cassandraColumn.getName(), 0);
        }
    }

    @Override
    public void setLongValue(Long value, BoundStatement statement)
    {
        statement.setFloat(cassandraColumn.getName(), value.floatValue());
    }

    @Override
    public void setDoubleValue(Double value, BoundStatement statement)
    {
        statement.setFloat(cassandraColumn.getName(), value.floatValue());
    }

    @Override
    public void setStringValue(String value, BoundStatement statement)
    {
        statement.setFloat(cassandraColumn.getName(), Float.parseFloat(value));
    }
}
