package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import org.embulk.spi.time.Timestamp;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class TimeColumnSetter extends CassandraColumnSetter
{

    public TimeColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setLongValue(Long value, BoundStatement statement)
    {
        statement.setTime(cassandraColumn.getName(), value);
    }

    @Override
    public void setDoubleValue(Double value, BoundStatement statement)
    {
        statement.setTime(cassandraColumn.getName(), value.longValue());
    }

    @Override
    public void setStringValue(String value, BoundStatement statement)
    {
        LocalTime time = LocalTime.parse(value);
        statement.setTime(cassandraColumn.getName(), time.toNanoOfDay());
    }

    @Override
    public void setTimestampValue(Timestamp value, BoundStatement statement)
    {
        ZonedDateTime datetime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(value.toEpochMilli()), ZoneId.of("UTC"));
        long nsec = datetime.toLocalTime().toNanoOfDay();

        statement.setTime(cassandraColumn.getName(), nsec);
    }
}
