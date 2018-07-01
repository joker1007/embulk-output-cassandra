package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;

public class BooleanColumnSetter extends CassandraColumnSetter
{
    public BooleanColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatement statement)
    {
        statement.setBool(cassandraColumn.getName(), value);
    }

    @Override
    public void setLongValue(Long value, BoundStatement statement)
    {
        statement.setBool(cassandraColumn.getName(), value != 0);
    }

    @Override
    public void setDoubleValue(Double value, BoundStatement statement)
    {
        statement.setBool(cassandraColumn.getName(), value != 0);
    }
}
