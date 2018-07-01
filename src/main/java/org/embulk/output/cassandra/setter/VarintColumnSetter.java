package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;

import java.math.BigInteger;

public class VarintColumnSetter extends CassandraColumnSetter
{
    public VarintColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatement statement)
    {
        if (value) {
            statement.setVarint(cassandraColumn.getName(), BigInteger.ONE);
        }
        else {
            statement.setVarint(cassandraColumn.getName(), BigInteger.ZERO);
        }
    }

    @Override
    public void setLongValue(Long value, BoundStatement statement)
    {
        statement.setVarint(cassandraColumn.getName(), BigInteger.valueOf(value));
    }

    @Override
    public void setDoubleValue(Double value, BoundStatement statement)
    {
        statement.setVarint(cassandraColumn.getName(), BigInteger.valueOf(value.longValue()));
    }

    @Override
    public void setStringValue(String value, BoundStatement statement)
    {
        statement.setVarint(cassandraColumn.getName(), new BigInteger(value));
    }
}
