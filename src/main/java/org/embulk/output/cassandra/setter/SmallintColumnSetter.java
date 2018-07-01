package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;

public class SmallintColumnSetter extends CassandraColumnSetter
{

    public SmallintColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatement statement)
    {
        if (value) {
            statement.setShort(cassandraColumn.getName(), (short) 1);
        }
        else {
            statement.setShort(cassandraColumn.getName(), (short) 0);
        }
    }

    @Override
    public void setLongValue(Long value, BoundStatement statement)
    {
        statement.setShort(cassandraColumn.getName(), value.shortValue());
    }

    @Override
    public void setDoubleValue(Double value, BoundStatement statement)
    {
        statement.setShort(cassandraColumn.getName(), value.shortValue());
    }

    @Override
    public void setStringValue(String value, BoundStatement statement)
    {
        statement.setShort(cassandraColumn.getName(), Short.parseShort(value));
    }
}
