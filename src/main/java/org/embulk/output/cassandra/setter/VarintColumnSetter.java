package org.embulk.output.cassandra.setter;


import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

import java.math.BigInteger;

public class VarintColumnSetter extends CassandraColumnSetter
{
    public VarintColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatementBuilder statement)
    {
        if (value) {
            statement.setBigInteger(cassandraColumn.getName(), BigInteger.ONE);
        }
        else {
            statement.setBigInteger(cassandraColumn.getName(), BigInteger.ZERO);
        }
    }

    @Override
    public void setLongValue(Long value, BoundStatementBuilder statement)
    {
        statement.setBigInteger(cassandraColumn.getName(), BigInteger.valueOf(value));
    }

    @Override
    public void setDoubleValue(Double value, BoundStatementBuilder statement)
    {
        statement.setBigInteger(cassandraColumn.getName(), BigInteger.valueOf(value.longValue()));
    }

    @Override
    public void setStringValue(String value, BoundStatementBuilder statement)
    {
        statement.setBigInteger(cassandraColumn.getName(), new BigInteger(value));
    }
}
