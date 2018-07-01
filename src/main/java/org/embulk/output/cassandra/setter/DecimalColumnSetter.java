package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import org.embulk.spi.time.Timestamp;

import java.math.BigDecimal;

public class DecimalColumnSetter extends CassandraColumnSetter
{

    public DecimalColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatement statement)
    {
        if (value) {
            statement.setDecimal(cassandraColumn.getName(), BigDecimal.valueOf(1));
        }
        else {
            statement.setDecimal(cassandraColumn.getName(), BigDecimal.valueOf(0));
        }
    }

    @Override
    public void setLongValue(Long value, BoundStatement statement)
    {
        statement.setDecimal(cassandraColumn.getName(), BigDecimal.valueOf(value));
    }

    @Override
    public void setDoubleValue(Double value, BoundStatement statement)
    {
        statement.setDecimal(cassandraColumn.getName(), BigDecimal.valueOf(value));
    }

    @Override
    public void setStringValue(String value, BoundStatement statement)
    {
        statement.setDecimal(cassandraColumn.getName(), new BigDecimal(value));
    }

    @Override
    public void setTimestampValue(Timestamp value, BoundStatement statement)
    {
        statement.setDecimal(cassandraColumn.getName(), BigDecimal.valueOf(value.getEpochSecond()));
    }
}
