package org.embulk.output.cassandra.setter;


import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

public class SmallintColumnSetter extends CassandraColumnSetter
{
    public SmallintColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatementBuilder statement)
    {
        if (value) {
            statement.setShort(cassandraColumn.getName(), (short) 1);
        }
        else {
            statement.setShort(cassandraColumn.getName(), (short) 0);
        }
    }

    @Override
    public void setLongValue(Long value, BoundStatementBuilder statement)
    {
        statement.setShort(cassandraColumn.getName(), value.shortValue());
    }

    @Override
    public void setDoubleValue(Double value, BoundStatementBuilder statement)
    {
        statement.setShort(cassandraColumn.getName(), value.shortValue());
    }

    @Override
    public void setStringValue(String value, BoundStatementBuilder statement)
    {
        statement.setShort(cassandraColumn.getName(), Short.parseShort(value));
    }
}
