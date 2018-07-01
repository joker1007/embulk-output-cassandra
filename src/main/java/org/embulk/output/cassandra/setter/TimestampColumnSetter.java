package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import org.embulk.spi.time.Timestamp;

import java.util.Date;

public class TimestampColumnSetter extends CassandraColumnSetter
{

    public TimestampColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setLongValue(Long value, BoundStatement statement)
    {
        Date time = new Date(value * 1000);
        statement.setTimestamp(cassandraColumn.getName(), time);
    }

    @Override
    public void setDoubleValue(Double value, BoundStatement statement)
    {
        Date time = new Date(value.longValue() * 1000);
        statement.setTimestamp(cassandraColumn.getName(), time);
    }

    @Override
    public void setStringValue(String value, BoundStatement statement)
    {
        statement.setString(cassandraColumn.getName(), value);
    }

    @Override
    public void setTimestampValue(Timestamp value, BoundStatement statement)
    {
        Date time = new Date(value.toEpochMilli());
        statement.setTimestamp(cassandraColumn.getName(), time);
    }
}
