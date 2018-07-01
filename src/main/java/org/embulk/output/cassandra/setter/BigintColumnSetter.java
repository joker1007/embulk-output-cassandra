package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;

public class BigintColumnSetter extends CassandraColumnSetter
{

    public BigintColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatement statement)
    {
        if (value) {
            statement.setLong(cassandraColumn.getName(), 1);
        }
        else {
            statement.setLong(cassandraColumn.getName(), 0);
        }
    }

    @Override
    public void setLongValue(Long value, BoundStatement statement)
    {
        statement.setLong(cassandraColumn.getName(), value);
    }

    @Override
    public void setDoubleValue(Double value, BoundStatement statement)
    {
        statement.setLong(cassandraColumn.getName(), value.longValue());
    }

    @Override
    public void setStringValue(String value, BoundStatement statement)
    {
        statement.setLong(cassandraColumn.getName(), Long.parseLong(value));
    }
}
