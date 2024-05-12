package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

import java.time.Instant;
import java.time.LocalDate;

public class DateColumnSetter extends CassandraColumnSetter
{
    public DateColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setStringValue(String value, BoundStatementBuilder statement)
    {
        LocalDate date = LocalDate.parse(value);
        statement.setLocalDate(cassandraColumn.getName(), date);
    }

    @Override
    public void setTimestampValue(Instant value, BoundStatementBuilder statement)
    {
        LocalDate date = LocalDate.from(value);
        statement.setLocalDate(cassandraColumn.getName(), date);
    }
}
