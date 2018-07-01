package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import org.embulk.spi.time.Timestamp;

import java.time.LocalDate;

public class DateColumnSetter extends CassandraColumnSetter
{

    public DateColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setStringValue(String value, BoundStatement statement)
    {
        LocalDate date = LocalDate.parse(value);
        statement.setDate(cassandraColumn.getName(), com.datastax.driver.core.LocalDate.fromYearMonthDay(date.getYear(), date.getMonthValue(), date.getDayOfMonth()));
    }

    @Override
    public void setTimestampValue(Timestamp value, BoundStatement statement)
    {
        com.datastax.driver.core.LocalDate date = com.datastax.driver.core.LocalDate.fromMillisSinceEpoch(value.toEpochMilli());
        statement.setDate(cassandraColumn.getName(), date);
    }
}
