package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import org.embulk.spi.time.Timestamp;

public class DoubleColumnSetter extends CassandraColumnSetter
{

    public DoubleColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatement statement)
    {
        if (value) {
            statement.setDouble(cassandraColumn.getName(), 1);
        }
        else {
            statement.setDouble(cassandraColumn.getName(), 0);
        }
    }

    @Override
    public void setLongValue(Long value, BoundStatement statement)
    {
        statement.setDouble(cassandraColumn.getName(), value.doubleValue());
    }

    @Override
    public void setDoubleValue(Double value, BoundStatement statement)
    {
        statement.setDouble(cassandraColumn.getName(), value);
    }

    @Override
    public void setStringValue(String value, BoundStatement statement)
    {
        statement.setDouble(cassandraColumn.getName(), Double.parseDouble(value));
    }

    @Override
    public void setTimestampValue(Timestamp value, BoundStatement statement)
    {
        statement.setDouble(cassandraColumn.getName(), value.getEpochSecond());
    }
}
