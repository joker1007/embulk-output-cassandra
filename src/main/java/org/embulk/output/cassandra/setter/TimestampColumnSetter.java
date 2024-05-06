package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

import java.time.Instant;
import java.util.Date;

public class TimestampColumnSetter extends CassandraColumnSetter
{
    public TimestampColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setLongValue(Long value, BoundStatementBuilder statement)
    {
        statement.setInstant(cassandraColumn.getName(), Instant.ofEpochSecond(value));
    }

    @Override
    public void setDoubleValue(Double value, BoundStatementBuilder statement)
    {
        long longValue = value.longValue();
        long nanoSecond = Math.round((value - longValue) * 1_000_000_000);
        statement.setInstant(cassandraColumn.getName(), Instant.ofEpochSecond(longValue, nanoSecond));
    }

    @Override
    public void setStringValue(String value, BoundStatementBuilder statement)
    {
        statement.setString(cassandraColumn.getName(), value);
    }

    @Override
    public void setTimestampValue(Instant value, BoundStatementBuilder statement)
    {
        statement.setInstant(cassandraColumn.getName(), value);
    }
}
